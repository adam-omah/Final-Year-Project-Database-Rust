# Use the official Rust image as the base image
FROM rust:1.84.0 AS builder

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
COPY --from=builder /app/target/release/Final-Year-Project-Database-Rust /app/Final-Year-Project-Database-Rust

# Copy other necessary files (like database directory or schema files)
COPY static ./static
COPY config.yaml ./config.yaml


# Expose the port on which the Actix Web server runs
ARG PORT=8080
EXPOSE ${PORT}

# Set default environment variables
ENV PORT=${PORT}
ENV CONFIG_PATH=/app/config.yaml



# Command to run the application
CMD ["./Final-Year-Project-Database-Rust"]