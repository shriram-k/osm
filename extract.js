const { Sequelize, DataTypes } = require("sequelize");
const fs = require("fs");
const path = require("path");
const Parser = require("osm-pbf-parser");

// Configuration
const OSM_DATA_URL = "https://download.geofabrik.de/north-america/canada-latest.osm.pbf";
const DATA_DIR = "./data";
const OSM_FILE = path.join(DATA_DIR, "canada-latest.osm.pbf");

// Database Configuration
const sequelize = new Sequelize("mydatabase", "myuser", "mypassword", {
  host: "localhost",
  dialect: "mysql",
  logging: false,
  pool: {
    max: 10,
    min: 0,
    acquire: 30000,
    idle: 10000,
  },
});

// Define the Address model
const Address = sequelize.define("address", {
  latitude: { type: DataTypes.DECIMAL(10, 8), allowNull: false },
  longitude: { type: DataTypes.DECIMAL(11, 8), allowNull: false },
  housenumber: { type: DataTypes.STRING, allowNull: true },
  housename: { type: DataTypes.STRING, allowNull: true },
  street: { type: DataTypes.STRING, allowNull: true },
  suburb: { type: DataTypes.STRING, allowNull: true },
  city: { type: DataTypes.STRING, allowNull: true },
  district: { type: DataTypes.STRING, allowNull: true },
  state: { type: DataTypes.STRING, allowNull: true },
  province: { type: DataTypes.STRING, allowNull: true },
  postcode: { type: DataTypes.STRING, allowNull: true },
  country: { type: DataTypes.STRING, allowNull: true },
  place: { type: DataTypes.STRING, allowNull: true },
  full: { type: DataTypes.TEXT, allowNull: true },
});

// Format the full address
function formatAddress(address) {
  return `${address.housenumber ? address.housenumber + " " : ""}${address.street}, ${address.suburb ? address.suburb + ", " : ""}${address.city ? address.city + ", " : ""}${address.state ? address.state + ", " : ""}${address.postcode ? address.postcode + ", " : ""}${address.country}`;
}

// Process a single OSM node
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

  address.full = formatAddress(address);
  return address;
}

// Parse the OSM PBF file and save to the database
async function processOSMFile() {
  console.log("Processing OSM data...");

  const parser = new Parser();
  const records = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(OSM_FILE)
      .pipe(parser)
      .on("data", async (items) => {
        for (const item of items) {
          if (item.type === "node") {
            const tags = item.tags || {};

            const processedAddress = processNode(item);
            records.push(processedAddress);

            if (records.length >= 1000) {
              try {
                await Address.bulkCreate(records);
                console.log("Saved 100 records to the database...");
                records.length = 0; // Clear the batch
              } catch (error) {
                console.error("Error saving records:", error);
                reject(error);
              }
            }
          }
        }
      })
      .on("end", async () => {
        try {
          if (records.length > 0) {
            await Address.bulkCreate(records);
          }
          console.log("All data saved to the database.");
          resolve();
        } catch (error) {
          console.error("Error saving remaining records:", error);
          reject(error);
        }
      })
      .on("error", (error) => {
        console.error("Error parsing OSM file:", error.message);
        reject(error);
      });
  });
}

// Main function
async function main() {
  try {
    console.log("Starting...");
    await sequelize.sync({ force: true }); // Reset database schema
    console.log("Database synchronized.");

    // Uncomment if OSM data needs to be downloaded
    // await downloadOSMData();

    await processOSMFile();
    console.log("Processing complete.");
  } catch (error) {
    console.error("Error during main execution:", error.message);
  } finally {
    await sequelize.close();
    console.log("Database connection closed.");
  }
}

main();
