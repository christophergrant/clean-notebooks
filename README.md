## Launch a Cluster On Your Workspace

In order to run any of the Cleanroom Notebooks, you’ll need to launch a small cluster in your Databricks Workspace.  After launching the cluster, there will be a Libraries tab available in the cluster’s configuration.  The databricks-cli is required for a few of the Notebooks - you can install it on the cluster by going to the Libraries tab, clicking “Install new”, clicking on PyPI and typing in: `databricks-cli`. After the library is installed you’ll attach this cluster to the Cleanroom Notebooks discussed below.


## Onboarding


### Pre-Work
- Give Databricks the URL for your workspace, a GCS bucket that the Databricks cleanroom will use to pull data and write results and control tables, and the participant name you want to use
- The Cleanroom’s Google Service account must be given access to the GCS bucket - it needs to list, read, create, modify and delete objects in the bucket.


### Onboarding
- Create a directory in your Databricks workspace under repos called Cleanroom
- Connect that directory to the  <https://github.com/christophergrant/clean-notebooks.git> repo
- Open the 00_onboarding Notebook, under the “onboarding” subdirectory
- Run only the first cell of the Notebook - widgets will appear at the top
- Fill in the values for the GCS bucket and participant name that was given to Databricks during the pre-work.  They must be the same values.
- Click away from the widgets, then click “Run All”.  You’ll see directories and one Delta table get created in your GCS bucket, along with two databases in your Databricks Workspace’s metastore.  Please do not modify any of these settings.
- Databricks will enable something on the Cleanroom side, and you’ll see more Delta tables appear in your GCS bucket in the control directory.
- Once Databricks completes initialization on the Cleanroom side, open the 01_onboarding Notebook, under the “onboarding” subdirectory
- Run only the first cell of the Notebook - a widget will appear at the top
- Fill in the value for your GCS bucket
- Click “Run All”.  This will register the rest of your control tables in your Databricks Workspace’s Metastore
- At this point onboarding is complete, and you can start using the other Notebooks to register data and code, submit jobs, and if you are the keeper of the Cleanroom approve code submitted from other Cleanroom participants.


## Registering Data
In order to process data in the Cleanroom, you need to register it so the Cleanroom can find it.  To register a dataset you’re going to use the register_code Notebook.  Here are the steps you need to follow to register a dataset with the Cleanroom:- Put a dataset in your GCS bucket that the Cleanroom has access to in the input directory.  It is strongly encouraged to use Delta format, but Parquet, JSON and Avro can also be used
- In one of your own Notebooks, register your dataset in your Workspace’s metastore with a CREATE TABLE statement, and specify the location of your dataset with the location parameter.  Documentation for registering a Delta table in your local Workspace’s metastore is [here](https://docs.databricks.com/delta/quick-start.html#create-a-table).  The dataset must be registered in the database that has the same name as your participant name that was created for you during initialization.
- Open the register_code Notebook
- If there are no widgets displayed yet then run only the first cell of the Notebook - widgets will appear at the top
- Fill in the name of the table in data_metastore_name.  For example, if your Cleanroom participant name is acme and your table’s name is test1, you would fill in acme.test1 in the widget.
- Fill in a description for the table.
- Select the operation.  If this is the first time you’re registering this dataset you would select INSERT.  If you’re updating it, either by changing the location or in the case of non-Delta tables changing the schema then select UPDATE.  If you’re removing the dataset from the cleanroom then select DELETE.
- Click away from the widgets, then click “Run All”
- After a few minutes the record will appear in your control.data table and you’ll be able to use that table in your cleanroom code.  If you do not see it after a few minutes, check your control.event_status table to see if there was an error registering the data.


## Registering Code
Code must also be registered with the Cleanroom so that the Cleanroom can run jobs with it.  It is also necessary so that it can go through an approval process if needed (more on this below).  To register code you’ll use the register_code Notebook.  At this time the code must be in a Databricks Notebook for the Cleanroom to run it, but in the future it will take any code that a Databricks Workspace can run.  Here are the steps you need to follow:- Create and test your code in a Databricks Notebook in your local workspace.   The Notebook can be parameterized using Notebook Widgets, so that parameters can be passed in to the Notebook at runtime.  Documentation on widgets can be found [here](https://docs.databricks.com/notebooks/widgets.html).
- Open the register_code Notebook.
- If there are no widgets displayed yet then run only the first cell of the Notebooks - widgets will appear at the top
- Fill in a unique name for this code.  It will be checked for global uniqueness, so add something distinctive to the front of the code name, such as your participant name
- Fill in a description
- Fill in the path to the Notebook in your workspace.  The path will start with /Users, /Repos or / and a top-level directory that you’ve created in your Workspace
- Fill in the operation - either INSERT, UPDATE or DELETE
- Fill in any default parameters that you want the Notebook to have
- Click away from the widgets, then click “Run All”
- After a few minutes the record will appear in your control.code table and your control.approval_status table.  If you do not see it after a few minutes, check your control.event_status table to see if there was an error registering the code.
- If you accidentally re-submit code with the same name with INSERT as the action, it will return an error to the control.event_status table saying that the code name is not unique.  Make sure to use UPDATE or DELETE if you want to modify code you have already registered
- If someone other than the Cleanroom keeper is registering the code, it will need to be approved by the keeper.  See “Approving Code” for more information.


## Approving Code
When a collaborator other than the keeper registers code, the keeper must approve it before it can be used in the Cleanroom.  If a record appears in the control.approval_status with an approval_status value of CODE_REQUEST_PENDING_APPROVAL, then that code needs to be approved.  Here are the steps you need to follow:- Open the approve_code Notebook
- Click “Run All”, in this case it does not matter if widgets are visible yet
- At the bottom of the Notebook, if there is code that needs to be reviewed there will be a list of entries showing the name, description, hash and submitter of the code
- You can import the code into your workspace and examine it before approving.  The code will be in your cloud storage bucket under the structure /code/participant/code name/hash.  Download the code from the cloud storage bucket, and then you can use the Databricks “Import Notebook” functionality to import the code so it can be examined.
- When you’re ready to approve code, change the action widget to “APPROVE”
- The code_name widget is a multi-select with the list of code names that need approval.  Check one or more of the code names.
- Fill in a reason.
- Click “Run All”.  Requests to approve the selected sets of code will be sent to the Cleanroom, and the widgets will be reset so that only code left to be approved will be in the list.
- If there is no code left to approve, the bottom of the Notebook will say “There are no pending code requests. Please check back later.”
- The same process can be used to reject code - just select “REJECT” in the action widget instead of “APPROVE”
- After a few minutes the control.approval_status record will be updated to show the new status for that code.  If the code was approved, it can now be used in a Cleanroom job.


## Submitting a Job
Once data and code have been registered and code has been approved, a job can be submitted to the Cleanroom that uses the approved code to process the data.  Here are the steps you need to follow:- Open the create_job Notebook
- Unlike the other Notebooks, at this time you cannot use “Run All”. in this Notebook.  The Notebook is being reworked to make this simpler.
- First, you must run the first cell of the Notebook, and then the second cell of the Notebook, to reset the widgets and refresh the list of code in the code_name widget
- Give the job a name and select the code you want to use from the dropdown.
- Enter in the values for any parameters that the code requires.  The values are in the form of a JSON, for example {“output”: “gs://mybucket/output/mytable”}.  Note that the format of the parameters must be valid JSON, with double quotes and parameters separated by commas.
- Select the compute size from the dropdown.  The larger the amount of data that will be processed by the job, the larger the compute size that should be picked.
- Click on and run the third cell of the Notebook so that the widget values are grabbed.
- Click on and run the fourth cell of the Notebook.  The request will be sent to the Cleanroom.
- Check the control.event_status table to verify that the job was submitted successfully.
- After a few minutes you will see the status of the job appear in the control.runs table.  The status will be updated until the job is completed.  You’ll see the output appear in the output location that was specified as a parameter in the job.


## Definitions of Control Tables And How To Read Them
You can query each of the tables in the control schema in your Databricks workspace to get information about what has been submitted to the Cleanroom.  Please never attempt to modify these tables - only the Cleanroom Notebooks and Cleanroom processes should modify them.


### control.approval_status
This table contains one record for each set of code that is registered with the Cleanroom.  If you are the keeper you will see the approval status of all of your code plus the approval status of the code of all of your collaborators.  If you are a collaborator you will see the approval status of all your code plus all the code submitted by the keeper.Only code with a status of CODE_REQUEST_APPROVED can be used in a Cleanroom job.


### control.code
This table contains one record for each set of code that is registered with the Cleanroom.  If you are the keeper you will see all of your code plus the code of all of your collaborators.  If you are a collaborator you will see all of your code plus all the code submitted by the keeper.


### control.data
This table contains one record for each dataset that has been registered with the Cleanroom.  If you are the keeper you will see all of your datasets plus the datasets of all of your collaborators.  If you are a collaborator you will see all of your datasets plus the datasets submitted by the keeper.


### control.event_status
This table will show you the status of each request that was sent to and processed by the Cleanroom.  If there was a problem with a request an error will be reported.


### control.events
This is the table that the control Notebooks all write to.  You can see a complete history of all of your requests to the Cleanroom here, the different types of the requests and who and when they were submitted.  The Cleanroom automatically pulls new records from this table to process the Cleanroom requests.


### control.jobs
This table contains one record for each job that has been submitted to the Cleanroom.  If you are the keeper you will see all of your jobs plus the jobs of all of your collaborators.  If you are a collaborator you will see only your jobs.


### control.runs
This table contains the status of the jobs that have been submitted to the Cleanroom.  The process that populates it runs every few minutes to check on the status of jobs that are currently running in the Databricks Cleanroom or that have run in the past 30 days.  It will show when a given job completes and also show if it has errored out and provide an error message.There is a run_page_url field that has a URL to the job being run in the Cleanroom.  If you try to go to that URL you will be denied access, but if Databricks is helping you debug a job then with your permission they will access the Cleanroom and be able to see what happened with a given job.
