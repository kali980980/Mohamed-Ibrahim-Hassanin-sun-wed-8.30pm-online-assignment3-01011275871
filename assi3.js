//Read a file in chunks using a Readable Stream
const fs = require('fs');

// Create a readable stream
const readableStream = fs.createReadStream('./big.txt', { encoding: 'utf8' });

// Listen for the 'data' event to handle each chunk
readableStream.on('data', (chunk) => {
    console.log('--- Received Chunk ---');
    console.log(chunk);
});

readableStream.on('end', () => {
    console.log('Finished reading file.');
});

readableStream.on('error', (err) => {
    console.error('Error reading file:', err.message);
});

//Copy content using Readable and Writable Streams
const fs = require('fs');

const sourceStream = fs.createReadStream('./source.txt');
const destStream = fs.createWriteStream('./dest.txt');

// Pipe the read stream directly into the write stream
sourceStream.pipe(destStream);

destStream.on('finish', () => {
    console.log('File copied using streams');
});

sourceStream.on('error', (err) => console.error('Read error:', err.message));
destStream.on('error', (err) => console.error('Write error:', err.message));

//. Stream Pipeline with Compression (gzip)

const fs = require('fs');
const zlib = require('zlib');
const { pipeline } = require('stream');

const source = fs.createReadStream('./data.txt');
const gzip = zlib.createGzip();
const destination = fs.createWriteStream('./data.txt.gz');

// Use pipeline to safely handle errors and close streams properly
pipeline(source, gzip, destination, (err) => {
    if (err) {
        console.error('Pipeline failed:', err.message);
    } else {
        console.log('Pipeline succeeded: File compressed to data.txt.gz');
    }
});

//HTTP Server Code (User Registration with Duplicate Email Check)
const http = require('http');
const fs = require('fs');
const path = require('path');

const FILE_PATH = path.join(__dirname, 'users.json');
const PORT = 3000;

const server = http.createServer((req, res) => {
    // 1. Create an API that adds a new user
    if (req.url === '/users' && req.method === 'POST') {
        let body = '';

        // Collect incoming stream chunks
        req.on('data', chunk => {
            body += chunk.toString();
        });

        req.on('end', () => {
            try {
                const newUser = JSON.parse(body);

                // Validation check for required fields
                if (!newUser.email || !newUser.name) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    return res.end(JSON.stringify({ error: 'Name and Email are required' }));
                }

                // Read current file data directly
                fs.readFile(FILE_PATH, 'utf8', (err, data) => {
                    let users = [];
                    
                    if (!err && data) {
                        try {
                            users = JSON.parse(data);
                        } catch (parseErr) {
                            users = []; // Handle empty or malformed JSON
                        }
                    }

                    // Check if email already exists
                    const emailExists = users.some(user => user.email === newUser.email);
                    if (emailExists) {
                        res.writeHead(400, { 'Content-Type': 'application/json' });
                        return res.end(JSON.stringify({ error: 'Email already exists' }));
                    }

                    // Assign an ID and add to file data structure
                    newUser.id = Date.now();
                    users.push(newUser);

                    // Write updated dataset back to the JSON file
                    fs.writeFile(FILE_PATH, JSON.stringify(users, null, 2), (writeErr) => {
                        if (writeErr) {
                            res.writeHead(500, { 'Content-Type': 'application/json' });
                            return res.end(JSON.stringify({ error: 'Failed to write data' }));
                        }

                        res.writeHead(201, { 'Content-Type': 'application/json' });
                        res.end(JSON.stringify({ message: 'User created successfully', user: newUser }));
                    });
                });

            } catch (error) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Invalid JSON format' }));
            }
        });

    } else {
        // Handle Fallback 404
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Route not found' }));
    }
});

server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});



const express = require('express');
const fs = require('fs/promises');
const path = require('path');

const app = express();
app.use(express.json()); // Middleware to parse JSON request bodies

const filePath = path.join(__dirname, 'users.json');

// Helper function to read users directly from the JSON file
async function readUsersFile() {
    try {
        const data = await fs.readFile(filePath, 'utf8');
        return JSON.parse(data || '[]');
    } catch (error) {
        // If file doesn't exist, return an empty array
        if (error.code === 'ENOENT') {
            return [];
        }
        throw error;
    }
}

// Helper function to save users directly to the JSON file
async function writeUsersFile(users) {
    await fs.writeFile(filePath, JSON.stringify(users, null, 2), 'utf8');
}

// ==========================================
// 1. Create a user (POST /user)
// ==========================================
app.post('/user', async (req, { body }, res) => {
    const { name, age, email } = body;

    try {
        const users = await readUsersFile();

        // Ensure that the email of the new user doesn't exist before
        const emailExists = users.some(u => u.email === email);
        if (emailExists) {
            return res.status(400).json({ message: "Email already exists." });
        }

        // Auto-increment ID based on the last element or start at 1
        const newId = users.length > 0 ? users[users.length - 1].id + 1 : 1;
        const newUser = { id: newId, name, age, email };

        users.push(newUser);
        await writeUsersFile(users);

        res.status(201).json({ message: "User added successfully." });
    } catch (error) {
        res.status(500).json({ message: "Server error", error: error.message });
    }
});

// ==========================================
// 2. Update a user by ID (PATCH /user/:id)
// ==========================================
app.patch('/user/:id', async (req, res) => {
    const targetId = parseInt(req.params.id, 10);
    const updates = req.body;

    try {
        const users = await readUsersFile();
        const userIndex = users.findIndex(u => u.id === targetId);

        if (userIndex === -1) {
            return res.status(404).json({ message: "User ID not found." });
        }

        // Merge existing user data with the updates provided in the request body
        users[userIndex] = { ...users[userIndex], ...updates };
        await writeUsersFile(users);

        res.status(200).json({ message: "User age updated successfully." });
    } catch (error) {
        res.status(500).json({ message: "Server error", error: error.message });
    }
});

// ==========================================
// 3. Delete a user by ID (DELETE /user/:id)
// ==========================================
app.delete('/user/:id', async (req, res) => {
    const targetId = parseInt(req.params.id, 10);

    try {
        const users = await readUsersFile();
        const userIndex = users.findIndex(u => u.id === targetId);

        if (userIndex === -1) {
            return res.status(404).json({ message: "User ID not found." });
        }

        // Remove the user at the found index
        users.splice(userIndex, 1);
        await writeUsersFile(users);

        res.status(200).json({ message: "User deleted successfully." });
    } catch (error) {
        res.status(500).json({ message: "Server error", error: error.message });
    }
});

// ==========================================
// 4. Get all users (GET /user)
// ==========================================
app.get('/user', async (req, res) => {
    try {
        const users = await readUsersFile();
        res.status(200).json(users);
    } catch (error) {
        res.status(500).json({ message: "Server error", error: error.message });
    }
});

// ==========================================
// 5. Get user by ID (GET /user/:id)
// ==========================================
app.get('/user/:id', async (req, res) => {
    const targetId = parseInt(req.params.id, 10);

    try {
        const users = await readUsersFile();
        const user = users.find(u => u.id === targetId);

        if (!user) {
            return res.status(404).json({ message: "User not found." });
        }

        res.status(200).json(user);
    } catch (error) {
        res.status(500).json({ message: "Server error", error: error.message });
    }
});

// Start the server
const PORT = 3000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});




// //Node.js Architecture & Core Concepts
// 1. What is the Node.js Event Loop?
// The Event Loop is the core mechanism that allows Node.js to perform non-blocking I/O operations despite JavaScript being single-threaded. It works by offloading operations (like network requests or file reading) to the system kernel whenever possible. It continuously monitors the execution stack and the callback queue, orchestrating when asynchronous callbacks should be executed on the main thread.

// 2. What is Libuv and What Role Does It Play in Node.js?
// Libuv is a multi-platform C library originally developed for Node.js.

// Its Role: It handles the low-level asynchronous plumbing of Node.js. It manages the system's event notifications, provides the abstraction for non-blocking network and file system operations, and implements both the Event Loop and the Thread Pool.

// 3. How Does Node.js Handle Asynchronous Operations Under the Hood?
// Node.js uses a combination of the V8 JavaScript engine and Libuv:

// Delegation: When an async task (like fs.readFile) is called, JavaScript passes it over to Libuv and moves on.

// Execution: Libuv delegates the task to either the operating system kernel (for network tasks) or its internal Thread Pool (for file/crypto tasks).

// Callback Registration: Once the task finishes, Libuv places the associated callback function into the Event Queue, waiting for the Event Loop to pick it up and run it on the main V8 thread.

// 4. Difference Between Call Stack, Event Queue, and Event Loop
// These three components work together to manage code execution: