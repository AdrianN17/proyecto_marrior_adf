import azure.functions as func
import logging
import os
from etl import run_etl_pipeline

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="")
def fx_pipelineETL(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        hotel_name = req_body.get('HotelName')
        
        if not hotel_name:
            return func.HttpResponse(
                "Error: HotelName es requerido en el request body",
                status_code=400
            )

        connection_string = os.getenv('ConnectionString')
        folder_source = os.getenv('FolderSource')
        folder_sink = os.getenv('FolderSink')
        server = os.getenv('SqlServer')
        database = os.getenv('SqlDatabase')
        username = os.getenv('SqlUsername')
        password = os.getenv('SqlPassword')
        
        run_etl_pipeline(connection_string, folder_source, folder_sink,
                        server, database, username, password, hotel_name)

        return func.HttpResponse(f"This HTTP triggered function executed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error en fx_pipelineETL: {str(e)}")
        return func.HttpResponse(
             f"Error: {str(e)}",
             status_code=500
        )


@app.route(route="fx_pipelineETL_POST", methods=["POST"])
def fx_pipelineETL_POST(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        hotel_name = req_body.get('HotelName')
        
        if not hotel_name:
            return func.HttpResponse(
                "Error: HotelName es requerido en el request body",
                status_code=400
            )

        connection_string = os.getenv('ConnectionString')
        folder_source = os.getenv('FolderSource')
        folder_sink = os.getenv('FolderSink')
        server = os.getenv('SqlServer')
        database = os.getenv('SqlDatabase')
        username = os.getenv('SqlUsername')
        password = os.getenv('SqlPassword')
        
        run_etl_pipeline(connection_string, folder_source, folder_sink,
                        server, database, username, password, hotel_name)

        return func.HttpResponse(f"This HTTP triggered function executed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error en fx_pipelineETL_POST: {str(e)}")
        return func.HttpResponse(
             f"Error: {str(e)}",
             status_code=500
        )