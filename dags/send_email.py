from airflow.models import Variable
import smtplib
from email.mime.text import MIMEText
from Earthquake_ETL import conectar_redshift, insert_data

email_user = Variable.get("email_user")
email_pass = Variable.get("email_pass")

def send_email(ti):
    magnitude = ti.xcom_pull(key="magnitude_earthquake")
    if(ti.xcom_pull(key="earthquakes")):
        msg = MIMEText(ti.xcom_pull(key="body"))
        msg['Subject'] = f"Últimos sismos - Magnitudes mayores a {magnitude} escala richter"
        msg['From'] = email_user
        msg['To'] = ', '.join([email_user])
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
            smtp_server.login(email_user, email_pass)
            smtp_server.sendmail(email_user, [email_user], msg.as_string())
        print("Message sent!")
    else:
        print(f"No hubieron sismos mayores a {magnitude} grados en la escala de richter")