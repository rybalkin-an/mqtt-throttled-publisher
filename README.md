# MQTT Publisher with Spread Distribution and Rate Limiting

A high-performance MQTT publisher tool that publishes realistic sensor data to multiple endpoints with configurable per-endpoint rate limiting and spread distribution patterns.

## Features

- **Per-Endpoint Rate Limiting**: Each endpoint respects individual rate limits (default: 17.65 seconds)
- **Spread Distribution**: Eligible messages are spread evenly over configurable intervals
- **Flexible Data Output**:
  - **Realistic Sensor Data**: Generates industrial sensor data (temperature, humidity, pressure, etc.) by default
  - set **`OUTPUT_DATA_TYPE=float`** for simplified JSON like `{ "value": 42.37 }`
  - set **`OUTPUT_DATA_TYPE=sequential`** for sequential JSON values like `{ "value": 1 }`, `{ "value": 2 }`, ...
- **Configurable Topic Template**:  
  Use placeholders like `{sensor_id}` to control the MQTT topic format.  
  By default topic is: `sensor/data/{sensor_id:04d}`  
  Example: `TOPIC_TEMPLATE="services/opcua/setSensor{sensor_id:04d}/set"`
- **High Performance**: Supports up to 2000 endpoints with ~112 msg/sec throughput
- **Docker Support**: Full containerization with Docker Compose
- **Flexible Configuration**: Extensive command-line and environment variable options
- **Monitoring**: Built-in statistics and logging

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐ 
│ Check Timer     │    │ Rate Limiter    │    │ Spread Engine   │ 
│ (5s default)    │───▶│ Per-endpoint    │───▶│ (30s default)   │
└─────────────────┘    │ tracking        │    │ distribution    │
                       └─────────────────┘    └─────────────────┘
                                               ▼ 
                                               ┌─────────────────┐ 
                                               │ MQTT Publisher  │ 
                                               │ (Paho Client    │ 
                                               └─────────────────┘       
                                               ▼
                                               ┌─────────────────┐ 
                                               │ MQTT Broker     │ 
                                               │ (Eclipse/etc.)  │ 
                                               └─────────────────┘
```

## Flow Rate Analysis

With default settings:
- **Endpoints**: 2000
- **Per-endpoint interval**: 17.65 seconds  
- **Theoretical maximum**: 113.31 msg/sec
- **Steady state**: ~113 msg/sec after initial ramp-up

## Quick Start

### 1. Using Docker Compose (Recommended)

```
docker-compose up -d
``` 

### 2. Using Docker Only

Build the image:

```bash 
 docker build -t mqtt-publisher .
```

Run with an external MQTT broker:

```bash
 docker run -d
--name mqtt-publisher
-e MQTT_BROKER="mqtt://your-broker:1883"
-e MQTT_USERNAME="admin"
-e MQTT_PASSWORD="admin"
-e ENDPOINTS="2000"
-e ENDPOINT_INTERVAL="17.65"
-v $(pwd)/logs:/app/logs
mqtt-publisher
``` 

### 3. Running Locally

```
pip install -r requirements.txt

python mqtt_publisher_rate_limited.py
--broker mqtt://localhost:1883
--username admin
--password admin
--endpoints 2000
--endpoint-interval 17.65
``` 

## Configuration

### Command Line Options

```bash
python mqtt_publisher_rate_limited.py --help

Key parameters:
- `--broker`: MQTT broker URL (default: mqtt://localhost:1883)
- `--endpoints`: Number of endpoints (default: 2000)
- `--client-id`: MQTT client ID (auto-generated if not specified)
- `--topic-template`: MQTT topic template (default: sensor/data/{sensor_id:04d})
- `--output-data-type`: Output data type ("sensor", "float", or "sequential", default: sensor)
- `--endpoint-interval`: Per-endpoint rate limit in seconds (default: 17.65)
- `--check-interval`: How often to check for eligible endpoints (default: 5.0)
- `--spread-interval`: Time to spread messages over (default: 30.0)
- `--max-messages-per-topic`: Stop each topic after N successful publishes (default: 0 = unlimited)
- `--qos`: MQTT QoS level 0-2 (default: 1)
- `--log-level`: Logging level (default: INFO)
```

### Environment Variables (Docker)

| Variable            | Default                     | Description                               |
|---------------------|-----------------------------|-------------------------------------------|
| `MQTT_BROKER`       | mqtt://localhost:1883       | MQTT broker URL                           |
| `MQTT_USERNAME`     | admin                       | MQTT username                             |
| `MQTT_PASSWORD`     | admin                       | MQTT password                             |
| `MQTT_CLIENT_ID`    | generated_id                | MQTT client id                            |
| `ENDPOINTS`         | 2000                        | Number of endpoints                       |
| `TOPIC_TEMPLATE`    | sensor/data/{sensor_id:04d} | Topic template (must include {sensor_id}) |
| `OUTPUT_DATA_TYPE`  | sensor                      | Output format: sensor (detailed JSON)<br/> float (random value)<br/> or sequential (1, 2, 3, ...) |
| `ENDPOINT_INTERVAL` | 17.65                       | Per-endpoint rate limit (seconds)         |
| `CHECK_INTERVAL`    | 5.0                         | Check frequency (seconds)                 |
| `SPREAD_INTERVAL`   | 30.0                        | Spread distribution time (seconds)        |
| `MAX_MESSAGES_PER_TOPIC` | 0                      | Max successful messages per topic (0 = unlimited) |
| `QOS`               | 1                           | MQTT QoS level                            |
| `LOG_LEVEL`         | INFO                        | Logging level                             |

## Data Formats

### Sensor mode by default (`OUTPUT_DATA_TYPE=sensor`)
The publisher generates realistic industrial sensor data:

```json 
{ "sensor_id": 1, 
  "sensor_type": "temperature",
  "timestamp": "2025-01-01T12:00:00.000000Z", 
  "temperature": 23.45, 
  "temperature_setpoint": 22.0, 
  "unit": "°C", 
  "location": { 
    "zone": "Zone_1", 
    "building": "Building_1", 
    "floor": 1, 
    "room": "Room_1"
  },
  "metadata": { 
    "unit_id": "UNIT_0001", 
    "manufacturer": "Siemens",
    "model": "Model_TEMPERATURE_123"
  }, 
  "status": "OK", 
  "quality": "GOOD"
}
``` 

Supported sensor types:
temperature, humidity, pressure, vibration, voltage, current, power, flow, level, speed.

### Float mode (`OUTPUT_DATA_TYPE=float`)

``` json
{ "value": 37.84 }
```

### Sequential mode (`OUTPUT_DATA_TYPE=sequential`)

```json
{ "value": 1 }
{ "value": 2 }
{ "value": 3 }
```

## Monitoring and Logs

### View Real-time Logs

```bash
 # Docker Compose
docker-compose logs -f mqtt-publisher

# Docker
docker logs -f mqtt-publisher

# Local
tail -f mqtt_publisher.log
``` 

### Statistics Output

The publisher reports statistics every 60 seconds:
```
=== Publisher Statistics === 
Connected: True T
otal messages published: 15000 
Total errors: 0 
Total dropped (rate limited): 2500 P
er-endpoint interval: 17.65s 
Spread interval: 30.0s 
Current publish rate: 112.0 msg/sec 
Success rate: 85.7% 
Drop rate: 14.3%
``` 

## Performance Tuning

### High-Volume Scenarios

```bash
 # Reduce per-endpoint interval for higher throughput
--endpoint-interval 10.0
# Increase spread interval for better distribution
--spread-interval 60.0
# More frequent checks
--check-interval 2.0
``` 

### Low-Volume Scenarios

```bash
 # Increase per-endpoint interval
--endpoint-interval 60.0
# Reduce checks frequency
--check-interval 30.0
``` 

## Troubleshooting

### Connection Issues

```bash
 # Check MQTT broker connectivity
docker-compose logs mqtt-broker

 # Test broker manually
mosquitto_pub -h localhost -p 1883 -u admin -P admin -t test -m "hello"
```

### High Drop Rates
- Increase `--endpoint-interval` to reduce rate limiting
- Increase `--spread-interval` to distribute load better
- Check broker capacity and connection stability

### Performance Issues
- Monitor Docker resources: `docker stats`
- Check network latency to MQTT broker
- Adjust `--qos` level (0 for the best performance, 2 for reliability)

## Development

### Building from Source

``` bash
# Clone repository
git clone <repo-url>
cd mqtt-publisher

# Build Docker image
docker build -t mqtt-publisher:latest .
```

### Custom Modifications

The main publisher class `MQTTDataPublisher` can be extended for custom behavior:
- Modify `generate_sensor_data()` for custom data formats
- Adjust `spread_publish_cycle()` for different distribution patterns
- Extend `print_statistics()` for custom metrics

## License
This project is licensed under the MIT License.

## Support
For issues and questions:
1. Check the troubleshooting section above
2. Review Docker Compose logs
3. Verify MQTT broker configuration
4. Test with reduced endpoint counts first

## Version History
- **v1.0.0**: Initial release with spread distribution and rate limiting
    - Support for 2000 endpoints
    - Per-endpoint rate limiting
    - Docker containerization
    - Realistic sensor data generation
- **v1.0.1**:
    - Added support for topic templating (`TOPIC_TEMPLATE`)
    - Added flexible output: choose between realistic industrial sensor data or simple float values (`OUTPUT_DATA_TYPE`)
    - Added setup client ID via `MQTT_CLIENT_ID` environment variable
- **v1.0.2**:
    - Added fallback to stdout when file logging fails (e.g., OpenShift read-only filesystem)
- **v1.0.3**:
    - Added `OUTPUT_DATA_TYPE=sequential` for incrementing values {"value": 1}, {"value": 2}, {"value": 3}, ...
```
