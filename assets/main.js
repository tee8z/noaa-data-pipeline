import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_wasm_next from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';

const MANUAL_BUNDLES = {
    mvp: {
        mainModule: duckdb_wasm,
        mainWorker: mvp_worker,
    },
    eh: {
        mainModule: duckdb_wasm_next,
        // mainWorker: new URL('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js', import.meta.url).toString(),
        mainWorker: eh_worker
    },
};

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);

// Instantiate the asynchronus version of DuckDB-wasm
const worker = new Worker(bundle.mainWorker);
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

const conn = await db.connect(); // Connect to db


document.addEventListener("DOMContentLoaded", function() {
    const apiBase = API_BASE || "https://example.com/api/";
    console.log("API_BASE:", apiBase);
    // Fetch file names
    fetchFileNames();

    // TODO: update the url to where ever the parquet_file_service is running
    function fetchFileNames() {
        fetch(`${apiBase}/files`)
            .then(response => response.json())
            .then(data => {
                const fileNames = data.file_names;
                populateDropdown(fileNames);
            })
            .catch(error => console.error("Error fetching file names:", error));
    }

    function populateDropdown(fileNames) {
        const dropdownContent = document.getElementById("dropdown-content");

        // Clear existing dropdown items
        dropdownContent.innerHTML = "";

        // Add new dropdown items based on file names
        fileNames.forEach(fileName => {
            const dropdownItem = document.createElement("a");
            dropdownItem.href = "#";
            dropdownItem.classList.add("dropdown-item");
            dropdownItem.textContent = fileName;

            dropdownContent.appendChild(dropdownItem);
        });
    }
});

/*
const pickedFile = letUserPickFile();
await db.registerFileHandle('local.parquet', pickedFile, DuckDBDataProtocol.BROWSER_FILEREADER, true);
// ...Remote
await db.registerFileURL('remote.parquet', 'https://origin/remote.parquet', DuckDBDataProtocol.HTTP, false);
// ... Using Fetch
const res = await fetch('https://origin/remote.parquet');
await db.registerFileBuffer('buffer.parquet', new Uint8Array(await res.arrayBuffer()));

// ..., by specifying URLs in the SQL text
await c.query(`
    CREATE TABLE direct AS
        SELECT * FROM "https://origin/remote.parquet"
`);
// ..., or by executing raw insert statements
await c.query(`INSERT INTO existing_table
    VALUES (1, "foo"), (2, "bar")`);
    */