const axios = require("axios");
const fs = require("fs");
const path = require("path");
const { createObjectCsvStringifier } = require("csv-writer");
const osmRead = require("osm-read");

// Configuration
const OSM_DATA_URL = "https://download.geofabrik.de/north-america/canada-latest.osm.pbf";
const DATA_DIR = "./data";
const OSM_FILE = path.join(DATA_DIR, "canada-latest.osm.pbf");
const timestamp = new Date().toISOString().replace(/[-:.]/g, "");
const ADDRESSES_FILE = path.join(DATA_DIR, `addresses_${timestamp}.csv`);

console.log(`CSV file will be saved as: ${ADDRESSES_FILE}`);

// Ensure data directory exists
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR);
}

const csvStringifier = createObjectCsvStringifier({
  header: [
    { id: "latitude", title: "Latitude" },
    { id: "longitude", title: "Longitude" },
    { id: "housenumber", title: "House Number" },
    { id: "housename", title: "House Name" },
    { id: "street", title: "Street" },
    { id: "suburb", title: "Suburb" },
    { id: "city", title: "City" },
    { id: "district", title: "District" },
    { id: "state", title: "State" },
    { id: "province", title: "Province" },
    { id: "postcode", title: "Postal Code" },
    { id: "country", title: "Country" },
    { id: "place", title: "Place" },
    { id: "full", title: "Full Address" }  // Add formatted full address
  ],
});

// Extract data and format the address
function formatAddress(address) {
  return `${address.housenumber ? address.housenumber + " " : ""}${address.street}, ${address.suburb ? address.suburb + ", " : ""}${address.city ? address.city + ", " : ""}${address.state ? address.state + ", " : ""}${address.postcode ? address.postcode + ", " : ""}${address.country}`;
}

function processNode(node) {
  const tags = node.tags || {};

  const address = {
    latitude: node.lat,
    longitude: node.lon,
    housenumber: tags["addr:housenumber"] || "",
    housename: tags["addr:housename"] || "",
    street: tags["addr:street"] || "",
    suburb: tags["addr:suburb"] || "",
    city: tags["addr:city"] || "",
    district: tags["addr:district"] || "",
    state: tags["addr:state"] || "",
    province: tags["addr:province"] || "",
    postcode: tags["addr:postcode"] || "",
    country: tags["addr:country"] || "",
    place: tags["addr:place"] || "",
  };

  // Format the full address
  const fullAddress = formatAddress(address);
  console.log(fullAddress);
  // Return the full address with individual components
  return {
    ...address,
    full: fullAddress,  // Add the formatted full address
  };
}

// Function to download OSM data
async function downloadOSMData() {
  console.log("Downloading OSM data...");
  const response = await axios({
    url: OSM_DATA_URL,
    method: "GET",
    responseType: "stream",
  });
  const writer = fs.createWriteStream(OSM_FILE);
  response.data.pipe(writer);
  return new Promise((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
}

function processOSMData() {
  console.log("Processing OSM data...");

  const csvWriteStream = fs.createWriteStream(ADDRESSES_FILE);
  csvWriteStream.write(csvStringifier.getHeaderString());

  osmRead.parse({
    filePath: OSM_FILE,
    format: "pbf",
    endDocument: () => {
      console.log("Processing complete.");
      csvWriteStream.end();
    },
    node: (node) => {
      const tags = node.tags || {};
      if(!tags["addr:postcode"] || !tags["addr:street"] || !tags["addr:city"] || !tags["addr:country"]) {
        return;
      }
      const processedAddress = processNode(node);
      csvWriteStream.write(csvStringifier.stringifyRecords([processedAddress]));
    },
  });
}

async function main() {
  try {
    // await downloadOSMData();
    processOSMData();
  } catch (error) {
    console.error("Error:", error.message);
  }
}

main();
