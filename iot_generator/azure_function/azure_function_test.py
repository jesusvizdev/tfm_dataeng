import json
import logging
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="LogLowRUL")
@app.route(route="LogLowRUL", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def log_low_rul(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    logging.info("LOW RUL ALERT RECEIVED")
    logging.info(json.dumps(payload))
    return func.HttpResponse("ok", status_code=200)
