const mysql = require('mysql2/promise');
const fetch = require('node-fetch');
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

async function updateBooksFromGoodreads() {
    try {
        // Get all books that haven't been processed
        const [books] = await pool.query(
            'SELECT * FROM nypl_books WHERE goodreads IS NOT TRUE'
        );

        for (const book of books) {
            try {
                // Construct Goodreads API URL
                const url = `https://www.goodreads.com/search/search?_extras%5Bbook_covers_large%5D=true&_nc=true&auto_search=1&format=xml&include_book_description=true&include_social_shelving_info=true&key=T7rSxXydAsZg0dU3PJzFhw&page=1&per_page=5&q=${book.isbn}&search%5Bfield%5D=all`;

                // Fetch data from Goodreads
                const response = await fetch(url);
                const xml = await response.text();

                // Parse XML response
                const parser = new (require('xml2js')).Parser();
                const result = await parser.parseStringPromise(xml);

                // Check if we got any results
                if (result.GoodreadsResponse?.search?.[0]?.results?.[0]?.work?.[0]?.best_book?.[0]) {
                    const bookData = result.GoodreadsResponse.search[0].results[0].work[0].best_book[0];
                    
                    // Update database with new information, including cover
                    await pool.query(
                        'UPDATE nypl_books SET title = ?, author = ?, summary = ?, cover = ?, goodreads = TRUE WHERE id = ?',
                        [
                            bookData.title[0],
                            bookData.author[0].name[0],
                            bookData.description[0],
                            bookData.large_image_url[0],
                            book.id
                        ]
                    );

                    console.log(`Updated book: ${book.isbn}`);
                } else {
                    // Mark as processed even if no results found
                    await pool.query(
                        'UPDATE nypl_books SET goodreads = TRUE WHERE id = ?',
                        [book.id]
                    );
                    console.log(`No Goodreads data found for ISBN: ${book.isbn}`);
                }

                // Add a small delay to avoid hitting rate limits
                await new Promise(resolve => setTimeout(resolve, 1000));
            } catch (error) {
                console.error(`Error processing ISBN ${book.isbn}:`, error);
            }
        }
    } catch (error) {
        console.error('Error in updateBooksFromGoodreads:', error);
    }
}

// Run the update function
updateBooksFromGoodreads();