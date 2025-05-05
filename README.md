# Satellite Telemetry Streaming and Computation Project

## Overview

This project streams real-time satellite telemetry data using the N2YO API and performs distributed computations using Apache Kafka and Apache Spark. The main objectives are:

- Compare Stream Vs. Batch processing via satellite position/path visualization.
- Calculate the satellite closest to a fixed observer location (e.g., Bangalore).
- Determine coverage overlap between multiple satellites.
- Compute motion vectors for each satellite based on positional data over time.

## Features

- Live satellite position data ingestion via the N2YO API
- Data publishing to Kafka topics
- Real-time computation using Spark Structured Streaming
- Derived insights based on spatial and temporal analysis
- Real-time satellite tracking
- Satellite Path visualization

## Technologies Used

- Python 3
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- REST APIs (N2YO)
- VS Code (Development environment)

## Computations

1. **Closest Satellite to Observer**
   - Compares real-time coordinates of satellites with a fixed observer (Delhi)
   - Uses haversine formula to determine distance

2. **Coverage Overlap**
   - Checks for overlapping ground footprints between satellite coverages
   - Requires assumptions about coverage radius

3. **Motion Vector Calculation**
   - Computes velocity vectors using consecutive latitude/longitude positions and timestamps

