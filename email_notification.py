import smtplib
import time


class EmailNotify:
    errors = 0

    def email_error_notification(self):

        try:
            sender_email = 'cts.itv@gmail.com'
            receiver_email = 'joeedwards88@gmail.com'
            password = "vphdzubvjwpjyido"
            message = f'Subject: RUNDOWN READER SERVER SHUTDOWN!\n\n' \
                      f'RR Server experienced 5/5 errors and has shutdown\n\nInvestigate immediately'
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message)

        except ConnectionRefusedError:
            print('Email not sent, conn refused...retrying in 30')
            self.errors += 1
            time.sleep(30)
            if self.errors <= 5:
                self.email_error_notification()
            else:
                print('Unable to send email - possible password expiry for cts.itv@gmail.com')
                return
