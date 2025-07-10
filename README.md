🔍 Lead Scoring System for CodePro
To streamline the sales process and eliminate inefficiencies caused by junk leads, we’ve built an automated Lead Scoring System that categorizes leads based on their likelihood of purchasing CodePro’s course. This system enables the sales team to prioritize high-quality leads and take data-driven actions.

🧱 System Architecture Overview
The solution is composed of three modular pipelines, each hosted in its own Docker container and orchestrated using Apache Airflow:

📊 1. Data Preprocessing Pipeline
Cleans and transforms raw lead data.

Maps categorical variables, handles missing values, and prepares interaction features.

Stores the processed data in a SQLite database for downstream tasks.

🎯 2. Training Pipeline
Picks up the processed data from the database.

Trains a LightGBM classification model to predict lead conversion likelihood.

Logs model artifacts and metrics using MLflow.

Saves the trained model in the designated models/ directory for reuse.

🔮 3. Inference Pipeline
Accepts new/unseen lead data for scoring.

Applies the same preprocessing steps used during training.

Loads the trained model to generate conversion predictions.

Stores the predictions and makes them available via a FastAPI endpoint.

🌐 Prediction Delivery via FastAPI
Inference results are POSTed to a FastAPI service, which stores and displays them.

A dedicated route allows users to view predictions in a browser-friendly HTML table, simplifying stakeholder access to scoring results.

⚙️ Technology Stack
Apache Airflow: Workflow orchestration

Docker: Isolated containerized pipelines

MLflow: Model tracking and management

FastAPI: Lightweight web server for exposing predictions

SQLite: Lightweight database for intermediate storage

LightGBM: Scalable, efficient model for lead scoring
