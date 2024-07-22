Instructions to Re-run Code and Verify Results:

1. Extract the contents of the archive to a suitable location on your machine.

2. Ensure you have Python and the required packages installed:
   - Python (>=3.6)
   - mrjob package (for the MapReduce code)
   - Jupyter Notebook (for the Jupyter Notebook code)
   - PySpark (for PySpark SQL in Jupyter Notebook)

3. MapReduce Code (MapReduce_FatalJob.py):

   To re-run the MapReduce code for identifying the earliest date with a "Power Good signal deactivated" error, follow these steps:

   a. Open a terminal or command prompt.

   b. Navigate to the directory where "x22104402_MapReduce.py" is located.

   c. Run the following command:
      ./x22104402_MapReduce.py -r inline BGL.log

   d. The script will execute the MapReduce job and output the earliest date with the specified error message.

4. Jupyter Notebook (x22104402_PySparkSQL.ipynb):

   To re-run the Jupyter Notebook for log analysis using PySpark SQL, follow these steps:

   a. Open Jupyter Notebook on your machine.

   b. Navigate to the directory where "x22104402_PySparkSQL.ipynb" is located.

   c. Open the "x22104402_PySparkSQL.ipynb" notebook.

   d. Run the notebook cells in sequence.

   e. The notebook will execute the PySpark SQL queries and display the results for the specified log analysis questions.

