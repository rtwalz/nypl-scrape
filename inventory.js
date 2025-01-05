const mysql = require('mysql2/promise');
const fetch = require('node-fetch');
const Limiter = require('async-limiter');
require('dotenv').config();

const pool = mysql.createPool({
    user: process.env.MYSQLUSER,
    password: process.env.MYSQLPASSWORD,
    port: process.env.MYSQLPORT,
    host: process.env.MYSQLHOST,
    database: "etc",
    waitForConnections: true,
    connectionLimit: 1,
    queueLimit: 0
});

// Add batch size constant
const BATCH_SIZE = 50;
const CONCURRENCY_LIMIT = 5;

async function checkInventory(id) {
    try {
        const response = await fetch(
            `https://na2.iiivega.com/api/search-result/drawer/${id}?tab=Book&locationCodes=jm`,
            {
                headers: {
                    'accept': 'application/json',
                    'anonymous-user-id': 'e100515c-ac6c-4e5d-859d-9c64e8aaf0c5',
                    'api-version': '1',
                    'iii-customer-domain': 'nypl.na2.iiivega.com',
                    'iii-host-domain': 'borrow.nypl.org'
                }
            }
        );

        const data = await response.json();
        const availableCount = data.items?.filter(
            item => item.status.availabilityStatus === "Available"
        ).length || 0;

        return availableCount;
    } catch (error) {
        console.error(`Error checking inventory for ID ${id}:`, error);
        return null;
    }
}

async function updateAllInventory() {
    try {
        const connection = await pool.getConnection();
        const limiter = new Limiter({ concurrency: CONCURRENCY_LIMIT });
        
        try {
            // Get current books with their inventory counts
            const [books] = await connection.query('SELECT id, inventory FROM nypl_books');
            console.log(`Found ${books.length} books to process`);
            
            // Create a map of current inventory levels
            const currentInventory = new Map(
                books.map(book => [book.id, book.inventory || 0])
            );
            
            // Process books in batches
            for (let i = 0; i < books.length; i += BATCH_SIZE) {
                const batch = books.slice(i, i + BATCH_SIZE);
                
                const results = await new Promise((resolve) => {
                    const batchResults = [];
                    let completed = 0;
                    
                    batch.forEach(book => {
                        limiter.push(async (done) => {
                            try {
                                const inventory = await checkInventory(book.id);
                                batchResults.push({ 
                                    id: book.id, 
                                    inventory,
                                    previousInventory: currentInventory.get(book.id) 
                                });
                            } catch (error) {
                                console.error(`Error processing book ${book.id}:`, error);
                            } finally {
                                completed++;
                                done();
                                if (completed === batch.length) {
                                    resolve(batchResults);
                                }
                            }
                        });
                    });
                });
                
                // Filter out null inventory results
                const validResults = results.filter(r => r.inventory !== null);
                
                if (validResults.length > 0) {
                    // Process inventory changes and create checkout records
                    const checkouts = [];
                    for (const result of validResults) {
                        const diff = (result.previousInventory || 0) - result.inventory;
                        if (diff > 0) {
                            // Add a checkout record for each decrease in inventory
                            for (let i = 0; i < diff; i++) {
                                checkouts.push([result.id, new Date()]);
                            }
                        }
                    }
                    
                    // Batch update inventory
                    const inventorySql = 'INSERT INTO nypl_books (id, inventory) VALUES ? ON DUPLICATE KEY UPDATE inventory = VALUES(inventory)';
                    const inventoryValues = validResults.map(r => [r.id, r.inventory]);
                    await connection.query(inventorySql, [inventoryValues]);
                    
                    // Insert checkout records if any exist
                    if (checkouts.length > 0) {
                        const checkoutSql = 'INSERT INTO nypl_checkouts (book_id, timestamp) VALUES ?';
                        await connection.query(checkoutSql, [checkouts]);
                        console.log(`Recorded ${checkouts.length} new checkouts`);
                    }
                }
                
                console.log(`Processed ${Math.min(i + BATCH_SIZE, books.length)} of ${books.length} books`);
            }
            
            console.log('Finished updating all inventory counts');
            
        } finally {
            connection.release();
        }
    } catch (error) {
        console.error('Database error:', error);
    }
}

// Run the update
updateAllInventory().finally(() => {
    pool.end();
    console.log('Process complete');
});