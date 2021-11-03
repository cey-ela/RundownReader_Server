import smtplib


def email_error_notification():

    sender_email = 'cts.itv@gmail.com'
    receiver_email = 'joeedwards88@gmail.com'
    password = "vphdzubvjwpjyido"
    message = f'Subject: RR SERVER ERROR! \n\n RR Server experienced error, please investigate'
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)
    print('email sent')

