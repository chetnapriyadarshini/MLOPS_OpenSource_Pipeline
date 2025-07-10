# main.py for Training Pipeline

import utils

def run_training_pipeline():
    print("\n[INFO] Starting Training Pipeline...")
    utils.encode_features()
    utils.get_trained_model()
    print("[INFO] Training Pipeline Completed.")

if __name__ == "__main__":
    run_training_pipeline()