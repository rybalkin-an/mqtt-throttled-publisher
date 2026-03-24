#!/usr/bin/env python3
"""
MQTT Publisher with Spread Distribution and Per-Endpoint Rate Limiting
Publishes data to 2000 endpoints with configurable per-endpoint rate limiting
Each endpoint can only send one message every X seconds (default: 17.65s)
Eligible messages are spread evenly over a configurable spread interval
"""

import json
import logging
import random
import threading
import time
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

# Configure logging
handlers = [logging.StreamHandler()]

try:
    handlers.append(logging.FileHandler('mqtt_publisher.log'))
except PermissionError:
    # Fall back gracefully, no crash (e.g. OpenShift read-only FS)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    logger = logging.getLogger(__name__)
    logger.warning(
        "Cannot write log file mqtt_publisher.log, continuing with stdout only"
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    logger = logging.getLogger(__name__)


class MQTTDataPublisher:
    """
    High-performance MQTT publisher with spread distribution and per-endpoint rate limiting.
    Each endpoint can only send one message every configurable interval (default: 17.65 seconds).
    Eligible messages are spread evenly over a configurable spread interval.
    Messages that violate rate limits are dropped.
    """

    def __init__(self, broker_url: str = "mqtt://localhost:1883",
                 username: str = "admin", password: str = "admin",
                 client_id: Optional[str] = None,
                 topic_template: str = "sensor/data/{sensor_id:04d}",
                 output_data_type: str = "sensor"):
        """
        Initialize the MQTT publisher with connection parameters.

        Args:
            broker_url (str): MQTT broker URL (e.g., "mqtt://broker:1883")
            username (str): MQTT username for authentication
            password (str): MQTT password for authentication
            client_id (Optional[str]): Custom client ID (auto-generated if None)
            topic_template (str): Format string for MQTT topic.
                Must include `{sensor_id}` placeholder.
                Example: "services/opcua/setNode{sensor_id}/set"
            output_data_type (str): Output data format.
                - "sensor" → detailed industrial sensor JSON (default)
                - "float" → simplified JSON like {"value": <float 0–60>}
                - "sequential" → simplified JSON like {"value": 1}, {"value": 2}, ...
        """
        self.broker_url = broker_url
        self.username = username
        self.password = password
        self.client_id = client_id or f"mqtt_publisher_{int(time.time())}"
        self.topic_template = topic_template
        self.output_data_type = output_data_type

        # Connection settings
        self.connected = False
        self.connection_event = threading.Event()
        self.client: Optional[mqtt.Client] = None

        # Publisher settings
        self.num_endpoints = 2000
        self.publish_qos = 1  # QoS level for publishing
        self.retain_messages = False

        # Per-endpoint rate limiting settings
        self.endpoint_interval = 17.65  # Configurable interval per endpoint
        self.endpoint_last_publish = {}  # Track last publish time per endpoint
        self.endpoint_last_publish_lock = threading.Lock()  # Thread-safe access
        self.max_messages_per_topic = 0  # 0 means unlimited
        self.endpoint_success_count = {}  # Track successful messages per endpoint/topic
        self.endpoint_success_count_lock = threading.Lock()

        # Spread publishing settings
        self.check_interval = 5.0  # Check for eligible endpoints every 5 seconds
        self.spread_interval = 30.0  # Spread eligible messages over 30 seconds
        
        # Performance metrics
        self.publish_count = 0
        self.error_count = 0
        self.dropped_count = 0  # Track dropped messages due to rate limiting
        self.last_stats_time = 0
        self.last_stats_count = 0
        self.metrics_lock = threading.Lock()
        self.sequential_values = {}  # Track sequential value per sensor
        self.sequential_value_lock = threading.Lock()

        # Shutdown handling
        self.shutdown_event = threading.Event()
        self.setup_signal_handlers()

        # Parse broker URL
        self._parse_broker_url()

    def _parse_broker_url(self):
        """Parse broker URL to extract host, port, and protocol."""
        try:
            parsed = urlparse(self.broker_url)
            self.host = parsed.hostname or "localhost"
            self.port = parsed.port or 1883
            self.use_tls = parsed.scheme == "mqtts"

            logger.info(f"Parsed broker URL - Host: {self.host}, Port: {self.port}, TLS: {self.use_tls}")
        except Exception as e:
            logger.error(f"Failed to parse broker URL {self.broker_url}: {e}")
            raise

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def set_endpoint_interval(self, interval_seconds: float):
        """Set the minimum interval between messages for each endpoint."""
        self.endpoint_interval = interval_seconds
        logger.info(f"Endpoint interval set to {interval_seconds:.2f} seconds")

    def can_publish_to_endpoint(self, sensor_id: int) -> bool:
        """
        Check if enough time has passed since last publish to this endpoint.
        
        Args:
            sensor_id: Sensor ID to check
            
        Returns:
            bool: True if endpoint can publish, False if rate limited
        """
        if self.max_messages_per_topic > 0:
            with self.endpoint_success_count_lock:
                sent_count = self.endpoint_success_count.get(sensor_id, 0)
                if sent_count >= self.max_messages_per_topic:
                    return False

        current_time = time.time()
        
        with self.endpoint_last_publish_lock:
            last_publish = self.endpoint_last_publish.get(sensor_id, 0)
            time_since_last = current_time - last_publish
            return time_since_last >= self.endpoint_interval

    def all_topics_reached_limit(self) -> bool:
        """Return True when all endpoints reached max_messages_per_topic."""
        if self.max_messages_per_topic <= 0:
            return False

        return self.get_completed_topics_count() >= self.num_endpoints

    def get_completed_topics_count(self) -> int:
        """Get number of topics that already reached max_messages_per_topic."""
        if self.max_messages_per_topic <= 0:
            return 0

        with self.endpoint_success_count_lock:
            completed_topics = sum(
                1 for sensor_id in range(1, self.num_endpoints + 1)
                if self.endpoint_success_count.get(sensor_id, 0) >= self.max_messages_per_topic
            )
        return completed_topics

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response."""
        if rc == 0:
            logger.info(f"Successfully connected to MQTT broker: {self.broker_url}")
            logger.info(f"Client ID: {self.client_id}")
            logger.info(f"Session present: {flags.get('session present', False)}")
            self.connected = True
            self.connection_event.set()
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorised"
            }
            error_msg = error_messages.get(rc, f"Connection refused - unknown error code {rc}")
            logger.error(f"Failed to connect to MQTT broker: {error_msg}")
            self.connected = False

    def on_disconnect(self, client: mqtt.Client, userdata, rc):
        """Callback for when the client disconnects."""
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker (code: {rc})")
        else:
            logger.info("Cleanly disconnected from MQTT broker")

        self.connected = False
        self.connection_event.clear()

    def on_publish(self, client: mqtt.Client, userdata, mid):
        """Callback for when a message is successfully published."""
        with self.metrics_lock:
            self.publish_count += 1
        logger.debug(f"Message {mid} published successfully")

    def on_log(self, client: mqtt.Client, userdata, level, buf):
        """Callback for MQTT client logging."""
        if level == mqtt.MQTT_LOG_ERR:
            logger.error(f"MQTT Client Error: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            logger.warning(f"MQTT Client Warning: {buf}")
        elif level == mqtt.MQTT_LOG_DEBUG:
            logger.debug(f"MQTT Client Debug: {buf}")

    def connect(self) -> bool:
        """
        Establish connection to MQTT broker.

        Returns:
            bool: True if the connection is successful, False otherwise
        """
        try:
            # Create MQTT client
            self.client = mqtt.Client(client_id=self.client_id, clean_session=True)

            # Set callbacks
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.on_publish = self.on_publish
            self.client.on_log = self.on_log

            # Configure authentication
            if self.username and self.password:
                self.client.username_pw_set(self.username, self.password)
                logger.info(f"Authentication configured for user: {self.username}")

            # Configure TLS if needed
            if self.use_tls:
                self.client.tls_set()
                self.client.tls_insecure_set(True)
                logger.info("TLS encryption enabled")

            # Set connection options
            self.client.max_inflight_messages_set(100)  # Limit inflight messages

            # Connect to broker
            logger.info(f"Connecting to MQTT broker at {self.host}:{self.port}")
            self.client.connect(self.host, self.port, keepalive=60)

            # Start network loop
            self.client.loop_start()

            # Wait for connection with timeout
            if self.connection_event.wait(timeout=10):
                logger.info("MQTT connection established successfully")
                return True
            else:
                logger.error("Connection timeout - failed to connect within 10 seconds")
                return False

        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False

    def disconnect(self):
        """Disconnect from MQTT broker gracefully."""
        if self.client and self.connected:
            logger.info("Disconnecting from MQTT broker...")
            self.client.loop_stop()
            self.client.disconnect()

            # Wait a bit for clean disconnection
            time.sleep(1)

        self.connected = False
        logger.info("Disconnected from MQTT broker")

    def _generate_sensor_json(self, sensor_id: int) -> Dict[str, Any]:
        """
        Generate realistic sensor data for industrial applications.

        Args:
            sensor_id: Unique sensor identifier

        Returns:
            Dict containing sensor data
        """
        # Determine a sensor type based on ID for consistent data patterns
        sensor_type_map = {
            0: "temperature",
            1: "humidity",
            2: "pressure",
            3: "vibration",
            4: "voltage",
            5: "current",
            6: "power",
            7: "flow",
            8: "level",
            9: "speed"
        }

        sensor_type = sensor_type_map[sensor_id % 10]

        # Generate type-specific data with realistic ranges
        base_data = {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "location": {
                "zone": f"Zone_{(sensor_id - 1) // 100 + 1}",
                "building": f"Building_{(sensor_id - 1) // 500 + 1}",
                "floor": ((sensor_id - 1) % 100) // 20 + 1,
                "room": f"Room_{sensor_id % 50 + 1}"
            },
            "metadata": {
                "unit_id": f"UNIT_{sensor_id:04d}",
                "manufacturer": random.choice(["Siemens", "ABB", "Schneider", "Honeywell", "Emerson"]),
                "model": f"Model_{sensor_type.upper()}_{random.randint(100, 999)}",
                "firmware_version": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 99)}"
            }
        }

        # Add sensor-specific measurements
        if sensor_type == "temperature":
            base_data.update({
                "temperature": round(random.uniform(18.0, 35.0), 2),
                "temperature_setpoint": round(random.uniform(20.0, 25.0), 1),
                "unit": "°C",
                "alarm_high": 40.0,
                "alarm_low": 10.0
            })
        elif sensor_type == "humidity":
            base_data.update({
                "humidity": round(random.uniform(30.0, 80.0), 2),
                "humidity_setpoint": round(random.uniform(45.0, 55.0), 1),
                "unit": "%RH",
                "alarm_high": 85.0,
                "alarm_low": 25.0
            })
        elif sensor_type == "pressure":
            base_data.update({
                "pressure": round(random.uniform(980.0, 1020.0), 2),
                "pressure_setpoint": round(random.uniform(995.0, 1005.0), 1),
                "unit": "mbar",
                "alarm_high": 1050.0,
                "alarm_low": 950.0
            })
        elif sensor_type == "vibration":
            base_data.update({
                "vibration_rms": round(random.uniform(0.0, 10.0), 3),
                "vibration_peak": round(random.uniform(0.0, 15.0), 3),
                "frequency": round(random.uniform(10.0, 1000.0), 1),
                "unit": "mm/s",
                "alarm_high": 12.0
            })
        elif sensor_type == "voltage":
            base_data.update({
                "voltage_l1": round(random.uniform(220.0, 240.0), 1),
                "voltage_l2": round(random.uniform(220.0, 240.0), 1),
                "voltage_l3": round(random.uniform(220.0, 240.0), 1),
                "unit": "V",
                "alarm_high": 250.0,
                "alarm_low": 200.0
            })
        elif sensor_type == "current":
            base_data.update({
                "current_l1": round(random.uniform(0.5, 15.0), 2),
                "current_l2": round(random.uniform(0.5, 15.0), 2),
                "current_l3": round(random.uniform(0.5, 15.0), 2),
                "unit": "A",
                "alarm_high": 20.0
            })
        elif sensor_type == "power":
            base_data.update({
                "active_power": round(random.uniform(100.0, 3600.0), 1),
                "reactive_power": round(random.uniform(50.0, 1000.0), 1),
                "power_factor": round(random.uniform(0.7, 0.99), 3),
                "unit": "W",
                "efficiency": round(random.uniform(85.0, 98.0), 2)
            })
        elif sensor_type == "flow":
            base_data.update({
                "flow_rate": round(random.uniform(10.0, 100.0), 2),
                "flow_total": round(random.uniform(1000.0, 50000.0), 1),
                "unit": "L/min",
                "alarm_high": 120.0,
                "alarm_low": 5.0
            })
        elif sensor_type == "level":
            base_data.update({
                "level_percent": round(random.uniform(10.0, 90.0), 2),
                "level_absolute": round(random.uniform(0.5, 4.5), 2),
                "unit": "%",
                "alarm_high": 95.0,
                "alarm_low": 5.0
            })
        elif sensor_type == "speed":
            base_data.update({
                "speed_rpm": round(random.uniform(1000.0, 3000.0), 1),
                "speed_setpoint": round(random.uniform(1500.0, 2500.0), 1),
                "unit": "RPM",
                "alarm_high": 3500.0,
                "alarm_low": 500.0
            })

        # Add common status information
        status_options = ["OK", "WARNING", "ERROR", "MAINTENANCE", "OFFLINE"]
        weights = [0.7, 0.15, 0.05, 0.08, 0.02]  # Weighted probability

        base_data.update({
            "status": random.choices(status_options, weights=weights)[0],
            "quality": random.choice(["GOOD", "UNCERTAIN", "BAD"]),
            "communication_status": "ONLINE" if random.random() > 0.02 else "TIMEOUT",
            "last_maintenance": (datetime.now(timezone.utc).timestamp() - random.randint(86400, 2592000)) * 1000,
            "next_maintenance": (datetime.now(timezone.utc).timestamp() + random.randint(86400, 7776000)) * 1000
        })

        return base_data

    def _generate_float_value(self, sensor_id: int) -> Dict[str, float]:
        return {"value": round(random.uniform(0.0, 60.0), 2)}

    def _generate_sequential_value(self, sensor_id: int) -> Dict[str, int]:
        with self.sequential_value_lock:
            current_value = self.sequential_values.get(sensor_id, 1)
            self.sequential_values[sensor_id] = current_value + 1
        return {"value": current_value}

    def generate_sensor_data(self, sensor_id: int) -> Dict[str, Any]:
        """Dispatch to generator based on output_data_type."""
        strategies = {
            "sensor": self._generate_sensor_json,
            "float": self._generate_float_value,
            "sequential": self._generate_sequential_value,
        }
        generator = strategies.get(self.output_data_type)
        if not generator:
            raise ValueError(f"Unsupported output_data_type: {self.output_data_type}")
        return generator(sensor_id)

    def publish_to_endpoint(self, sensor_id: int) -> bool:
        """
        Publish data to a single sensor endpoint with per-endpoint rate limiting.
        Returns False and drops message if rate limit is exceeded.

        Args:
            sensor_id: Sensor ID to publish data for

        Returns:
            bool: True if published successfully, False if rate limited or failed
        """
        if not self.connected:
            logger.debug(f"Not connected to broker, skipping sensor {sensor_id}")
            return False

        # Check per-endpoint rate limit
        if not self.can_publish_to_endpoint(sensor_id):
            with self.metrics_lock:
                self.dropped_count += 1
            logger.debug(f"Rate limit exceeded for sensor {sensor_id}, dropping message")
            return False

        try:
            # Generate topic and data
            topic = self.topic_template.format(sensor_id=sensor_id)
            data = self.generate_sensor_data(sensor_id)
            payload = json.dumps(data, separators=(',', ':'))  # Compact JSON

            # Publish message
            msg_info = self.client.publish(
                topic=topic,
                payload=payload,
                qos=self.publish_qos,
                retain=self.retain_messages
            )

            # Check if publish was successful
            if msg_info.rc == mqtt.MQTT_ERR_SUCCESS:
                # Update last publish time for this endpoint
                current_time = time.time()
                with self.endpoint_last_publish_lock:
                    self.endpoint_last_publish[sensor_id] = current_time
                with self.endpoint_success_count_lock:
                    self.endpoint_success_count[sensor_id] = self.endpoint_success_count.get(sensor_id, 0) + 1
                
                logger.debug(f"Published to {topic}: {len(payload)} bytes (MID: {msg_info.mid})")
                return True
            else:
                logger.error(f"Failed to publish to {topic}: {mqtt.error_string(msg_info.rc)}")
                with self.metrics_lock:
                    self.error_count += 1
                return False

        except Exception as e:
            logger.error(f"Exception publishing to sensor {sensor_id}: {e}")
            with self.metrics_lock:
                self.error_count += 1
            return False

    def get_eligible_endpoints(self) -> List[int]:
        """
        Get list of endpoints that are eligible to publish (not rate limited).
        
        Returns:
            List of sensor IDs that can publish now
        """
        eligible = []
        current_time = time.time()
        success_snapshot = {}

        if self.max_messages_per_topic > 0:
            with self.endpoint_success_count_lock:
                success_snapshot = self.endpoint_success_count.copy()

        with self.endpoint_last_publish_lock:
            for sensor_id in range(1, self.num_endpoints + 1):
                if self.max_messages_per_topic > 0:
                    sent_count = success_snapshot.get(sensor_id, 0)
                    if sent_count >= self.max_messages_per_topic:
                        continue

                last_publish = self.endpoint_last_publish.get(sensor_id, 0)
                time_since_last = current_time - last_publish
                if time_since_last >= self.endpoint_interval:
                    eligible.append(sensor_id)
        
        return eligible

    def generate_spread_schedule(self, eligible_endpoints: List[int]) -> List[Dict[str, Any]]:
        """
        Generate a spread schedule for eligible endpoints over the spread interval.
        
        Args:
            eligible_endpoints: List of sensor IDs that can publish
            
        Returns:
            List of dicts with sensor_id and publish_time
        """
        if not eligible_endpoints:
            return []
        
        schedule = []
        
        # Randomize the order for better distribution
        shuffled_endpoints = eligible_endpoints.copy()
        random.shuffle(shuffled_endpoints)
        
        # Spread endpoints evenly over the spread interval
        if len(shuffled_endpoints) == 1:
            # Single endpoint - publish immediately
            schedule.append({
                "sensor_id": shuffled_endpoints[0],
                "publish_time": 0.0
            })
        else:
            # Multiple endpoints - spread evenly
            time_between_publishes = self.spread_interval / len(shuffled_endpoints)
            
            for i, sensor_id in enumerate(shuffled_endpoints):
                publish_time = i * time_between_publishes
                schedule.append({
                    "sensor_id": sensor_id,
                    "publish_time": publish_time
                })
        
        # Sort by publish time for processing
        schedule.sort(key=lambda x: x["publish_time"])
        
        return schedule

    def execute_spread_schedule(self, schedule: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute the spread publishing schedule.
        
        Args:
            schedule: List of scheduled publications
            
        Returns:
            Dict with execution statistics
        """
        start_time = time.time()
        successful = 0
        failed = 0
        dropped = 0
        initial_dropped = self.dropped_count
        
        logger.debug(f"Executing spread schedule for {len(schedule)} endpoints over {self.spread_interval:.1f}s")
        
        for i, scheduled_item in enumerate(schedule):
            if self.shutdown_event.is_set():
                logger.info("Shutdown requested during spread execution")
                break
            
            sensor_id = scheduled_item["sensor_id"]
            scheduled_time = scheduled_item["publish_time"]
            
            # Wait until it's time for this publication
            current_elapsed = time.time() - start_time
            if current_elapsed < scheduled_time:
                sleep_time = scheduled_time - current_elapsed
                logger.debug(f"Waiting {sleep_time:.2f}s for sensor {sensor_id}")
                if self.shutdown_event.wait(timeout=sleep_time):
                    logger.info("Shutdown requested during spread wait")
                    break
            
            # Publish to this sensor
            if self.publish_to_endpoint(sensor_id):
                successful += 1
                logger.debug(f"Spread publish successful: sensor {sensor_id} ({i+1}/{len(schedule)})")
            else:
                # Check if it was dropped or failed
                current_dropped = self.dropped_count
                if current_dropped > initial_dropped:
                    dropped = current_dropped - initial_dropped
                else:
                    failed += 1
                logger.debug(f"Spread publish failed: sensor {sensor_id} ({i+1}/{len(schedule)})")
        
        duration = time.time() - start_time
        
        return {
            "scheduled_count": len(schedule),
            "successful": successful,
            "failed": failed,
            "dropped": dropped,
            "duration": duration,
            "target_duration": self.spread_interval
        }

    def spread_publish_cycle(self) -> Dict[str, Any]:
        """
        Perform one spread publish cycle - get eligible endpoints and spread them over interval.
        
        Returns:
            Dict with cycle statistics
        """
        cycle_start = time.time()
        
        # Get eligible endpoints
        eligible_endpoints = self.get_eligible_endpoints()
        
        if not eligible_endpoints:
            logger.debug("No eligible endpoints for publishing")
            return {
                "eligible_count": 0,
                "successful": 0,
                "failed": 0,
                "dropped": 0,
                "duration": time.time() - cycle_start
            }
        
        logger.info(f"Found {len(eligible_endpoints)} eligible endpoints, spreading over {self.spread_interval:.1f}s")
        
        # Generate spread schedule
        schedule = self.generate_spread_schedule(eligible_endpoints)
        
        # Execute the schedule
        execution_stats = self.execute_spread_schedule(schedule)
        
        # Combine stats
        cycle_duration = time.time() - cycle_start
        
        return {
            "eligible_count": len(eligible_endpoints),
            "scheduled_count": execution_stats["scheduled_count"],
            "successful": execution_stats["successful"],
            "failed": execution_stats["failed"],
            "dropped": execution_stats["dropped"],
            "duration": cycle_duration,
            "spread_duration": execution_stats["duration"],
            "spread_efficiency": (execution_stats["duration"] / self.spread_interval * 100) if self.spread_interval > 0 else 0
        }

    def print_statistics(self):
        """Print current publisher statistics including rate information."""
        current_time = time.time()
        
        with self.metrics_lock:
            total_attempts = self.publish_count + self.error_count + self.dropped_count
            
            # Calculate publish rate
            time_since_last_stats = current_time - self.last_stats_time
            messages_since_last_stats = self.publish_count - self.last_stats_count
            
            current_rate = 0
            if time_since_last_stats > 0:
                current_rate = messages_since_last_stats / time_since_last_stats
            
            logger.info("=== Publisher Statistics ===")
            logger.info(f"Connected: {self.connected}")
            logger.info(f"Total messages published: {self.publish_count}")
            logger.info(f"Total errors: {self.error_count}")
            logger.info(f"Total dropped (rate limited): {self.dropped_count}")
            logger.info(f"Per-endpoint interval: {self.endpoint_interval:.2f}s")
            logger.info(f"Spread interval: {self.spread_interval:.1f}s")
            if self.max_messages_per_topic > 0:
                completed_topics = self.get_completed_topics_count()
                logger.info(f"Max messages per topic: {self.max_messages_per_topic}")
                logger.info(f"Completed topics: {completed_topics}/{self.num_endpoints}")
            logger.info(f"Current publish rate: {current_rate:.1f} msg/sec")
            
            if total_attempts > 0:
                logger.info(f"Success rate: {(self.publish_count / total_attempts * 100):.1f}%")
                logger.info(f"Drop rate: {(self.dropped_count / total_attempts * 100):.1f}%")
            
            # Update last stats tracking
            self.last_stats_time = current_time
            self.last_stats_count = self.publish_count

    def run_publisher(self):
        """
        Main publisher loop with spread distribution and per-endpoint rate limiting.
        """
        logger.info(f"Starting MQTT Data Publisher (SPREAD + RATE_LIMITED mode)")
        logger.info(f"Configuration:")
        logger.info(f"  - Broker: {self.broker_url}")
        logger.info(f"  - Client ID: {self.client_id}")
        logger.info(f"  - Endpoints: {self.num_endpoints}")
        logger.info(f"  - Per-endpoint interval: {self.endpoint_interval}s")
        logger.info(f"  - Check interval: {self.check_interval}s")
        logger.info(f"  - Spread interval: {self.spread_interval}s")
        if self.max_messages_per_topic > 0:
            logger.info(f"  - Max messages per topic: {self.max_messages_per_topic}")
            logger.info(f"  - Total target messages: {self.num_endpoints * self.max_messages_per_topic}")
        else:
            logger.info("  - Max messages per topic: unlimited")
        logger.info(f"  - QoS: {self.publish_qos}")

        # Connect to broker
        if not self.connect():
            logger.error("Failed to connect to MQTT broker, exiting")
            return 1

        try:
            cycle_count = 0
            next_stats_time = time.time() + 60  # Print stats every 60 seconds
            self.last_stats_time = time.time()
            self.last_stats_count = 0

            logger.info("Starting spread publishing with rate limiting...")
            
            while not self.shutdown_event.is_set():
                cycle_count += 1
                
                logger.debug(f"Starting spread cycle #{cycle_count}")
                
                # Perform spread publish cycle
                cycle_stats = self.spread_publish_cycle()
                
                # Log cycle summary
                if cycle_stats["eligible_count"] > 0:
                    logger.info(f"Spread cycle #{cycle_count} completed: " +
                               f"{cycle_stats['eligible_count']} eligible, " +
                               f"{cycle_stats['successful']} published, " +
                               f"{cycle_stats['failed']} failed, " +
                               f"{cycle_stats['dropped']} dropped, " +
                               f"efficiency: {cycle_stats['spread_efficiency']:.1f}%")
                else:
                    logger.debug(f"Spread cycle #{cycle_count}: no eligible endpoints")

                if self.all_topics_reached_limit():
                    logger.info(
                        f"All topics reached limit ({self.max_messages_per_topic} messages/topic). Stopping publisher."
                    )
                    break
                
                # Print statistics periodically
                current_time = time.time()
                if current_time >= next_stats_time:
                    self.print_statistics()
                    next_stats_time = current_time + 60  # Next stats in 60 seconds
                
                # Wait before next cycle
                logger.debug(f"Waiting {self.check_interval:.1f}s until next check")
                if self.shutdown_event.wait(timeout=self.check_interval):
                    logger.info("Shutdown requested during check interval")
                    break

        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            logger.error(f"Unexpected error in publisher loop: {e}")
            return 1
        finally:
            # Clean shutdown
            self.disconnect()
            logger.info("Publisher shutdown complete")

        return 0


def main():
    """Main entry point with argument parsing."""
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description='MQTT Data Publisher with Spread Distribution and Per-Endpoint Rate Limiting',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Connection arguments (with environment variable defaults)
    parser.add_argument('--broker', 
                        default=os.getenv('MQTT_BROKER', 'mqtt://localhost:1883'),
                        help='MQTT broker URL')
    parser.add_argument('--username',
                        default=os.getenv('MQTT_USERNAME', 'admin'),
                        help='MQTT username')
    parser.add_argument('--password',
                        default=os.getenv('MQTT_PASSWORD', 'admin'),
                        help='MQTT password')
    parser.add_argument('--client-id',
                        default=os.getenv('MQTT_CLIENT_ID'),
                        help='MQTT client ID (auto-generated if not specified)')

    # Publisher arguments (with environment variable defaults)
    parser.add_argument('--endpoints', type=int,
                        default=int(os.getenv('ENDPOINTS', '2000')),
                        help='Number of endpoints to publish to')
    parser.add_argument('--endpoint-interval', type=float,
                        default=float(os.getenv('ENDPOINT_INTERVAL', '17.65')),
                        help='Minimum interval between messages per endpoint (seconds)')
    parser.add_argument('--check-interval', type=float,
                        default=float(os.getenv('CHECK_INTERVAL', '5.0')),
                        help='Interval between checking for eligible endpoints (seconds)')
    parser.add_argument('--spread-interval', type=float,
                        default=float(os.getenv('SPREAD_INTERVAL', '30.0')),
                        help='Interval over which to spread eligible messages (seconds)')
    parser.add_argument('--qos', type=int, choices=[0, 1, 2],
                        default=int(os.getenv('QOS', '1')),
                        help='MQTT QoS level')
    parser.add_argument('--retain', action='store_true',
                        help='Set retain flag on published messages')
    parser.add_argument('--topic-template',
                        default=os.getenv('TOPIC_TEMPLATE', 'sensor/data/{sensor_id:04d}'),
                        help='MQTT topic template (use {sensor_id} placeholder)')
    parser.add_argument('--output-data-type', choices=['sensor', 'float', 'sequential'],
                        default=os.getenv('OUTPUT_DATA_TYPE', 'sensor'),
                        help='Choose output data format: "sensor" (default), "float", or "sequential"')
    parser.add_argument('--max-messages-per-topic', type=int,
                        default=int(os.getenv('MAX_MESSAGES_PER_TOPIC', '0')),
                        help='Maximum successful messages per topic; 0 means unlimited')

    # Logging arguments
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default=os.getenv('LOG_LEVEL', 'INFO'),
                        help='Logging level')

    args = parser.parse_args()

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Create a publisher instance
    publisher = MQTTDataPublisher(
        broker_url=args.broker,
        username=args.username,
        password=args.password,
        client_id=args.client_id,
        topic_template=args.topic_template,
        output_data_type=args.output_data_type
    )

    # Configure publisher settings
    publisher.num_endpoints = args.endpoints
    publisher.endpoint_interval = args.endpoint_interval
    publisher.check_interval = args.check_interval
    publisher.spread_interval = args.spread_interval
    publisher.publish_qos = args.qos
    publisher.retain_messages = args.retain
    publisher.max_messages_per_topic = args.max_messages_per_topic

    # Validate settings
    if publisher.endpoint_interval <= 0:
        logger.error("Endpoint interval must be greater than 0")
        sys.exit(1)

    if publisher.check_interval <= 0:
        logger.error("Check interval must be greater than 0")
        sys.exit(1)

    if publisher.spread_interval <= 0:
        logger.error("Spread interval must be greater than 0")
        sys.exit(1)
    if publisher.max_messages_per_topic < 0:
        logger.error("Max messages per topic must be >= 0")
        sys.exit(1)

    # Calculate theoretical rates
    max_theoretical_rate = publisher.num_endpoints / publisher.endpoint_interval
    logger.info(f"Theoretical maximum publish rate: {max_theoretical_rate:.2f} msg/sec")

    spread_rate = publisher.num_endpoints / publisher.spread_interval
    logger.info(f"Maximum spread rate: {spread_rate:.2f} msg/sec")

    # Run publisher
    exit_code = publisher.run_publisher()
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
