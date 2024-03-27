from databricks_loganalytics.base_logging import log_console_output
from pyspark.sql import SparkSession, DataFrame
from databricks.sdk.runtime import *
from time import sleep
import os

global workspace_id
global workspace_key

workspace_id: str = os.environ.get('LOGGING_WORKSPACE_ID')
workspace_key: str = os.environ.get('LOGGING_WORKSPACE_KEY')

spark: SparkSession = SparkSession.builder.getOrCreate()

class notebook_logger:
    def __init__(self, pipeline_run_id: str, pipeline_name: str, activity_name: str):
        self.pipeline_run_id: str = pipeline_run_id
        self.pipeline_name: str = pipeline_name
        self.activity_name: str = activity_name


    def log_info(self, *messages) -> None:
        message: str = " ".join([str(m) for m in messages])
        dbx_body: dict[str, str] = [
            {
                "Message": message,
                "pipelineRunId_g": self.pipeline_run_id,
                "pipeline": self.pipeline_name,
                "activity": self.activity_name,
            }
        ]
        res: str = log_console_output(
            workspace_id=workspace_id,
            workspace_key=workspace_key,
            message=dbx_body,
        )
        print(message)
        if res != 'Accepted':
            print(res)
    

    def log_notebook_output(self, output: dict):

        def log_cmd_output(output):
            if isinstance(output, DataFrame):
                try:
                    out = output.collect()
                    if out:
                        outDict = out[0].asDict()
                        match len(outDict):
                            case 2:
                                operation: str = "INSERT"
                            case 4:
                                operation: str = "MERGE"
                            case 1:
                                operation: str = "DELETE or UPDATE"
                            case _:
                                operation: str = "UKNOWN"
                        if len(out) == 1:
                            message: str = f"{operation}: " + ", ".join([f"{op}: {str(cnt)}" for op, cnt in outDict.items()])
                            self.log_info(message)
                            sleep(0.1)
                except Exception as e:
                    # Handle any exceptions that may occur
                    print(f"WARNING: {output} not logged")
            elif isinstance(output, str):
                self.log_info(output)

        [log_cmd_output(values) for _, values in output.items()]


    def log_inserted_count(self, table):
        message: str = f"INSERTED {spark.read.table(table).count()}"
        self.log_info(message)
    
