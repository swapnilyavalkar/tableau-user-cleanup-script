---

# 🎯 Tableau Server User Cleanup Script

Welcome to the **Tableau Server User Cleanup Script**! This Python script is designed to automate the process of managing and cleaning up inactive or unnecessary users on your Tableau Server. It performs several key tasks to ensure your Tableau environment remains efficient and organized.

## 🚀 Features

- **Automated User Cleanup**: Identifies and marks inactive users as unlicensed.
- **Email Notifications**: Sends detailed email reports after successful, failed, or not required operations.
- **Detailed Logging**: Tracks all activities with comprehensive logs for troubleshooting.
- **Data Export**: Generates Excel files to document the cleanup process.

## 🛠️ Prerequisites

Before running this script, ensure you have the following:

- **Python 3.8+** installed
- **Required Python Libraries**: Install them using `pip install -r requirements.txt`
- **Tableau Server Client (TSC)**: The script uses the `tableauserverclient` library.
- **PostgreSQL**: Used for querying the Tableau PostgreSQL repository.
- **Email Configuration**: SMTP settings must be configured in the `Variables.py` file.

## 📋 Script Overview

This script follows a step-by-step process to clean up users on Tableau Server. Here’s a brief overview:

### 1. **Initialize Logging**
   - The script starts by configuring logging to track all operations.

### 2. **Sign in to PostgreSQL Database**
   - Connects to the Tableau PostgreSQL repository to fetch user data.

### 3. **User Cleanup Operations**
   - **Remove Inactive Users Based on Last Login**: Identifies users who haven’t logged in for a specified period.
   - **Remove Users Subscribed to Content**: Ensures users who have subscribed to content are not mistakenly unlicensed.
   - **Remove Content Owners**: Users owning flows, data sources, or workbooks are identified and handled separately.
   - **Mark Users as Unlicensed**: Updates user roles on Tableau Server to “Unlicensed” if they meet the criteria.

### 4. **Generate Reports**
   - The script generates Excel files documenting the changes and stores them in the `files` directory.

### 5. **Email Notification**
   - After completing the cleanup, the script sends an email with the results, detailing successes, failures, or indicating that no action was required.

### 6. **Close Connections**
   - Finally, the script closes all database connections and logs out from Tableau Server.

## 📂 Directory Structure

Here’s a quick overview of the key directories and files used by this script:

```bash
├── logs/                      # Log files generated during script execution
├── sql_queries/               # SQL queries used in the script
├── files/                     # Excel reports generated after cleanup
├── config.py                  # Configuration file for database connections
├── Variables.py               # Variables and constants used throughout the script
└── main.py          # Main Python script
```

## ⚙️ Configuration

Before running the script, configure the following files:

1. **config.py**: Contains database connection parameters.
2. **Variables.py**: Contains essential variables, such as Tableau credentials, SMTP settings, and other constants.

## 🚨 Error Handling

The script includes robust error handling. If any operation fails, it is logged in the `logs/` directory, and an email is sent with the details of the error.

## 📧 Email Notifications

- **Success**: An email is sent when the cleanup operation is successful.
- **Failure**: If any errors occur, a failure email is sent with detailed logs.
- **Not Required**: If no users need cleanup, an email is sent to inform that no action was required.

## 🧑‍💻 How to Run

1. **Clone the Repository**: Clone this repository to your local machine.
2. **Install Dependencies**: Run `pip install -r requirements.txt` to install necessary packages.
3. **Run the Script**: Execute the script using `python cleanup_script.py`.
4. **Review Logs and Reports**: Check the `logs/` directory for logs and the `files/` directory for Excel reports.

## 📝 Notes

- **Customize SQL Queries**: SQL queries can be customized based on your specific requirements. They are located in the `sql_queries/` directory.
- **Environment Variables**: Ensure all environment-specific variables are correctly set in `Variables.py`.

## 🛡️ License

This script is licensed under the MIT License. See the `LICENSE` file for more information.

---
