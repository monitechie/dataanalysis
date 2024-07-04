import subprocess

# Run fetch_data.py to fetch and store data in MongoDB
subprocess.run(["python", "mongopython.py"])

# Run analyze_data.py to process the data and save to CSV
subprocess.run(["python", "analyze.py"])
