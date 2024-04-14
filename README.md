# Databricks Job Manager

## Project Overview
The Databricks Job Manager is designed to optimize resource utilization within the Databricks environment by scheduling job executions strategically. The primary goal of this project is to ensure that jobs run only on weekdays or non-holiday dates to conserve computational resources. This approach not only saves costs but also aligns job execution with actual business usage needs.

## Motivation
In current Databricks setups, there is no native functionality to schedule jobs based on dynamic conditions such as checking for weekdays or holidays. This limitation can lead to unnecessary resource consumption and costs when jobs are triggered on days when their output is not needed. Furthermore, Databricks supports pipelining tasks within a single job but lacks the capability to create pipelines across multiple jobs.

## Objectives
- **Resource Optimization:** Ensure jobs run only when their output is necessary, avoiding weekends and holidays.
- **Historical Logging:** Collect and store logs of all job executions. This historical log data helps in easy monitoring and understanding the health and performance trends of each job over time.
- **Future Enhancements:** Plan to extend the project to support inter-job pipelining, allowing for more complex and efficient workflows where the output of one job can trigger the execution of another, something currently not supported natively in Databricks.

## How to Use
1. **Setup**: Clone the repository to your Databricks workspace.
2. **Configuration**: Update the `DATABRICKS_TOKEN` and `DATABRICKS_URL` in the script with your actual Databricks workspace API token and URL.
3. **Execution**: Run the script `databricks_job_manager.py` directly in your Databricks notebook or schedule it as a job to automate the process.
4. **Monitoring**: Check the generated logs in the specified path (e.g., `path/to/save/job_execution_log.parquet`) to monitor job status and health.

## Key Features
- Automatically skips job execution on weekends and public holidays in Brazil.
- Logs detailed execution history for audit and performance monitoring.
- Prepares for future capabilities to link multiple jobs into execution pipelines.

## Keywords
Databricks, Job Scheduling, Resource Management, Cost Optimization, Execution Logging, Python, API Integration, Holiday Check, Weekday Scheduling

## Future Work
We aim to enhance the capability of this tool by introducing features for inter-job dependencies and pipelining, enabling more complex workflows and efficient resource usage.

Feel free to contribute to this project by submitting pull requests or suggesting new features or improvements.

## License
This project is open-source and available under the MIT License.
