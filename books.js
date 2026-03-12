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

function cleanTitle(title) {
    // Remove everything after colon (including the colon)
    let cleanedTitle = title.split(/\s*[:=]/, 1)[0].trim();
    
    // Proper capitalization (first letter of each word)
    return cleanedTitle
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
}

function cleanAuthor(author) {
    if (!author) return null;
    
    // Remove dates and parenthetical information
    let cleanedAuthor = author.replace(/\(.*?\)/g, '')    // Remove parenthetical information
                             .replace(/,\s*\d+.*$/, '')    // Remove dates after comma
                             .replace(/\./g, '')           // Remove all periods
                             .replace(/\s+/g, ' ')         // Replace multiple spaces with single space
                             .trim();
    
    // If there's a comma, assume it's "Last, First" format
    if (cleanedAuthor.includes(',')) {
        const parts = cleanedAuthor.split(',').map(part => part.trim());
        cleanedAuthor = `${parts[1]} ${parts[0]}`; // First Last
    }
    
    // Proper capitalization
    return cleanedAuthor
        .split(' ')
        .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
        .join(' ');
}

async function fetchAndStoreBooksForYear(year) {
    let pageNum = 1;

    try {
        let totalProcessed = 0;
        const pageSize = 100;
        let totalPages = 1;

        console.log(`Starting fetch for publication year ${year}`);

        while (pageNum <= totalPages) {
            const response = await fetch('https://na2.iiivega.com/api/search-result/search/format-groups', {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'anonymous-user-id': 'e100515c-ac6c-4e5d-859d-9c64e8aaf0c5',
                    'api-version': '2',
                    'content-type': 'application/json',
                    'iii-customer-domain': 'nypl.na2.iiivega.com',
                    'iii-host-domain': 'borrow.nypl.org'
                },
                body: JSON.stringify({
                    searchText: "*",
                    sorting: "publicationDate",
                    sortOrder: "desc",
                    searchType: "everything",
                    universalLimiterIds: ["at_library"],
                    materialTypeIds: ["a"],
                    locationIds: ["jm"],
                    pageNum: pageNum,
                    pageSize: pageSize,
                    dateFrom: year.toString(),
                    dateTo: year.toString()
                })
            });

            if (!response.ok) {
                throw new Error(`NYPL API returned ${response.status} ${response.statusText}`);
            }

            const data = await response.json();
            totalPages = data.totalPages || totalPages;
            
            if (!data.data || data.data.length === 0) {
                break;
            }

            // Prepare all books data for bulk insert, filtering out incomplete records
            const booksData = data.data
                .filter(book => book.title)
                .map(book => [
                    book.id,
                    cleanTitle(book.title),
                    cleanAuthor(book.primaryAgent?.label || null),
                    book.identifiers?.isbn || null,
                    book.coverUrl?.medium || null
                ]);

            // Skip if no valid books in this batch
            if (booksData.length === 0) {
                console.log(`Page ${pageNum} had no valid books, continuing...`);
                pageNum++;
                continue;
            }

            // Bulk insert/update query
            const query = `
                INSERT INTO nypl_books (id, title, author, isbn, cover)
                VALUES ?
                ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    author = VALUES(author),
                    isbn = VALUES(isbn),
                    cover = VALUES(cover)
            `;

            await pool.query(query, [booksData]);

            totalProcessed += booksData.length;
            console.log(`Page ${pageNum} processed. Total books so far: ${totalProcessed}`);

            // Add a small delay to avoid overwhelming the API
            await new Promise(resolve => setTimeout(resolve, 1000));

            pageNum++;
        }

        console.log(`Finished processing publication year ${year}. Total processed: ${totalProcessed}`);
    } catch (error) {
        console.error(`Error fetching or storing books for publication year ${year}:`, error);
        console.error('Failed at page:', pageNum);
    }
}

async function fetchAndStoreBooks() {
    const currentYear = new Date().getFullYear();
    const startYear = Number(process.env.NYPL_START_YEAR || 1989);
    const endYear = Number(process.env.NYPL_END_YEAR || currentYear);

    try {
        if (!Number.isInteger(startYear) || !Number.isInteger(endYear)) {
            throw new Error('NYPL_START_YEAR and NYPL_END_YEAR must be whole numbers');
        }

        if (startYear > endYear) {
            throw new Error('NYPL_START_YEAR cannot be greater than NYPL_END_YEAR');
        }

        for (let year = endYear; year >= startYear; year--) {
            await fetchAndStoreBooksForYear(year);
            // Add a small delay between year ranges
            await new Promise(resolve => setTimeout(resolve, 2000));
        }
        
        console.log('Completed fetching books for all years');
    } finally {
        // Close the connection pool and exit the process
        await pool.end();
        process.exit(0);
    }
}

fetchAndStoreBooks().catch(error => {
    console.error('Fatal error:', error);
    pool.end().then(() => process.exit(1));
});
