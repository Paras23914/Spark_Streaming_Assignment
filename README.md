# Real-Time Stream Processing Assignment - Scenario A  

*Name:* Paras Sharma  
*ID:* 101040066

## Scenario Chosen

Scenario A – Autonomous Fleet Telemetry

## Dataset

AV GPS Dataset (GitHub)  
https://github.com/mehrab-abrar/AV-GPS-Dataset

## Objective

Detect sudden vehicle decelerations that may indicate accidents.

The pipeline computes the average vehicle speed using a 1-minute sliding window with a 10-second slide interval. An alert is generated when the average speed drops by more than 20 km/h between consecutive windows.

## Technologies Used

* Python 3.12
* Apache Spark Structured Streaming
* PySpark
* Java 17

## Project Structure

Assignment/  
├── preprocessing.py  
├── stream_job.py  
├── AV-GPS-Dataset-1.csv  
├── AV-GPS-Dataset-2.csv  
├── AV-GPS-Dataset-1.csv  
├── processed_data.csv  
├── incoming/  
│   └── processed_data.csv  
└── README.md  

## How to Run

### Step 1: Preprocess Data

```bash
python preprocessing.py
```

### Step 2: Start Streaming Job

```bash
py -3.12 stream_job.py
```

### Step 3: Place CSV File

Copy `processed_data.csv` into the `incoming` folder.

## Streaming Pipeline

Raw GPS Data
→ readStream
→ withWatermark("Timestamp", "1 minute")
→ Sliding Window (1 minute / 10 seconds)
→ Average Speed Calculation
→ Consecutive Window Comparison
→ Alert Generation

## Alert Condition

An alert is triggered when:

speed_drop > 20 km/h

where:

speed_drop = previous_window_avg_speed - current_window_avg_speed

## Why Sliding Window?

Data for vehicles is continuous. The sliding window method allows the observation of overlapping observations every 10 seconds, thus enabling the detection of sudden speed changes without having to wait for a full tumbling interval.

## Where State Is Required?

State is required for:

1. Maintaining records within the 1-minute sliding window.
2. Watermark management for late-arriving events.
3. Storing previous window averages to compare consecutive windows and detect speed drops.

## Sample Alert Output

Example detected anomalies:

* Vehicle 1 at 2022-04-05 11:00:30

* Speed drop: 113.84 km/h

* Vehicle 1 at 2022-04-06 13:59:20

* Speed drop: 91.15 km/h

Screenshots of alert generation are included with the submission.
<img width="1171" height="418" alt="image" src="https://github.com/user-attachments/assets/ad30fd8b-e926-478a-8e41-2649db29e5d3" />


