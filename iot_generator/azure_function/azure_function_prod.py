import azure.functions as func
import logging
import os
import smtplib
from email.mime.text import MIMEText
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="SendAlertEmail")
def SendAlertEmail(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("LOW RUL ALERT RECEIVED - SENDING EMAIL")

    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    logging.info(json.dumps(data))

    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    smtp_to   = "iot.sensoralerts.cmapss@gmail.com"

    if not smtp_user or not smtp_pass or not smtp_to:
        return func.HttpResponse("Faltan credenciales SMTP", status_code=500)

    subject = "LOW RUL ALERT DETECTED"
    body = f"Se ha detectado un RUL bajo:\n\n{json.dumps(data, indent=2)}"
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [smtp_to], msg.as_string())
        logging.info("Email enviado correctamente")
    except Exception as e:
        logging.error(f"Error enviando email: {e}")
        return func.HttpResponse(f"Error enviando email: {e}", status_code=500)

    return func.HttpResponse("Email enviado correctamente", status_code=200)
