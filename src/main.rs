use clap::{Parser, Subcommand};
use rusqlite::Connection;
use anyhow::{Context, Result, anyhow};

#[derive(Parser)]
#[command(name = "mbtile")]
#[command(about = "MBTiles utility", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract tiles from an MBTiles file
    Extract {
        /// Input MBTiles file
        input: String,

        /// Output MBTiles file
        output: String,

        /// Bounding box in format: N,E,S,W
        #[arg(long)]
        bbox: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Extract { input, output, bbox } => {
            if let Err(e) = extract_tiles(&input, &output, &bbox) {
                eprintln!("Error: {}", e);
                std::process::exit(1);
            }
        }
    }
}

#[derive(Debug)]
struct BoundingBox {
    north: f64,
    east: f64,
    south: f64,
    west: f64,
}

impl BoundingBox {
    fn parse(bbox_str: &str) -> Result<Self> {
        let parts: Vec<&str> = bbox_str.split(',').collect();
        if parts.len() != 4 {
            return Err(anyhow!("Bounding box must have 4 values: N,E,S,W"));
        }

        Ok(BoundingBox {
            north: parts[0].trim().parse().context("Invalid north value")?,
            east: parts[1].trim().parse().context("Invalid east value")?,
            south: parts[2].trim().parse().context("Invalid south value")?,
            west: parts[3].trim().parse().context("Invalid west value")?,
        })
    }

    fn tile_bounds(&self, zoom: i32) -> (i32, i32, i32, i32) {
        let n = 2_i32.pow(zoom as u32);

        // Convert lat/lon to tile coordinates (slippy map)
        let x_min = ((self.west + 180.0) / 360.0 * n as f64).floor() as i32;
        let x_max = ((self.east + 180.0) / 360.0 * n as f64).floor() as i32;

        let lat_rad = self.north.to_radians();
        let y_min = ((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n as f64).floor() as i32;

        let lat_rad = self.south.to_radians();
        let y_max = ((1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n as f64).floor() as i32;

        // Convert slippy map Y to TMS Y (flip)
        let tms_y_min = n - 1 - y_max;
        let tms_y_max = n - 1 - y_min;

        // Clamp to valid range
        (
            x_min.max(0).min(n - 1),
            x_max.max(0).min(n - 1),
            tms_y_min.max(0).min(n - 1),
            tms_y_max.max(0).min(n - 1)
        )
    }
}

fn extract_tiles(input_path: &str, output_path: &str, bbox_str: &str) -> Result<()> {
    let bbox = BoundingBox::parse(bbox_str)?;

    let output_conn = Connection::open(output_path)
        .context(format!("Failed to create output file: {}", output_path))?;

    // Create output schema
    output_conn.execute_batch(
        "CREATE TABLE metadata (name TEXT, value TEXT);
         CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB);
         CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);"
    )?;

    // Attach input database
    output_conn.execute(
        "ATTACH DATABASE ? AS input",
        rusqlite::params![input_path]
    )?;

    // Copy metadata
    output_conn.execute(
        "INSERT INTO metadata SELECT name, value FROM input.metadata",
        []
    )?;

    // Get all zoom levels present in the database
    let zoom_levels: Vec<i32> = {
        let mut stmt = output_conn.prepare("SELECT DISTINCT zoom_level FROM input.tiles ORDER BY zoom_level")?;
        stmt.query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
    };

    // Extract and copy tiles within bounding box for each zoom level
    let mut copied = 0;
    for zoom in zoom_levels {
        let (x_min, x_max, y_min, y_max) = bbox.tile_bounds(zoom);

        let rows = output_conn.execute(
            "INSERT INTO tiles SELECT zoom_level, tile_column, tile_row, tile_data FROM input.tiles
             WHERE zoom_level = ? AND tile_column BETWEEN ? AND ? AND tile_row BETWEEN ? AND ?",
            rusqlite::params![zoom, x_min, x_max, y_min, y_max]
        )?;
        copied += rows;
    }

    output_conn.execute("DETACH DATABASE input", [])?;

    println!("Extraction complete: {} tiles copied", copied);

    Ok(())
}
