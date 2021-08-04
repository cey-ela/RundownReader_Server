import smtplib
import json


def email_error_notification(source):
    with open("C:\\Program Files\\RundownReader_Server\\xyz\\aws_creds.json") as creds:
        email_dets = json.load(creds)
        sender_email_address = email_dets[2]['email']
        send_email_passwd = email_dets[2]['passwd']

    try:
        # Create your SMTP session
        smtp = smtplib.SMTP('smtp.gmail.com', 587)

        # Use TLS to add security
        smtp.starttls()

        # User Authentication
        smtp.login(sender_email_address, send_email_passwd)

        # Defining The Message
        body = source + ' CONSOLE IS NOT UPDATING RUNDOWN READER. INVESTIGATE IMMEDIATELY.'

        # Sending the Email
        smtp.sendmail("joeedwards88@gmail.com", "joeedwards88@gmail.com", body)

        # Terminating the session
        smtp.quit()
        print("Email sent successfully!")

    except Exception as ex:
        print("Something went wrong....", ex)