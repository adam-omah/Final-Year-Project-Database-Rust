# Use the official Rust image as the base image
FROM rust:1.84.0 as builder

# Set the working directory in the container
WORKDIR /app

# Copy all necessary files to the working directory
COPY . .

# Build the application in release mode
RUN cargo build --release

# Use a lighter image for the final container to reduce size
FROM debian:bookworm-slim

# Install required dependencies
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Set the working directory in the final container
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/Final_Year_Project_Database_Rust /app/Final_Year_Project_Database_Rust

# Copy other necessary files (like database directory or schema files)
COPY mydb ./mydb
COPY mydb/schema.json ./mydb/schema.json

# Expose the port on which the Actix Web server runs
EXPOSE 8080

# Command to run the application
CMD ["./Final_Year_Project_Database_Rust"]
# Replace <binary_name> with your actual binary name