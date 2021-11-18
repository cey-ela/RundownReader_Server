from kivy.config import Config
Config.set('graphics', 'width', '1200')  # These have to be unconventionally set before other imports
Config.set('graphics', 'height', '800')
Config.set('graphics', 'minimum_width', '1200')
Config.set('graphics', 'minimum_height', '800')
import os
import time
from threading import Thread
from kivymd.app import MDApp
from kivy.clock import Clock
from s3_connection import upload_to_aws
from datetime import datetime
from ftplib import FTP
from inews_pull_sort_push import InewsPullSortPush
import re
from kivy.uix.textinput import TextInput
import json
from kivy import properties as kp
from email_notification import EmailNotify


class ConsoleApp(MDApp):
    """
    ..the epicenter...
    Be mindful to always open and close FTP sessions to the iNews server properly. It is a production server sitting
    at the core of the Newsroom and arguably the most important.

    """
    gb_log = kp.ListProperty()  # lists used to store the output console text
    lk_log = kp.ListProperty()  # ^
    tm_log = kp.ListProperty()  # ^
    lw_log = kp.ListProperty()  # ^
    cs_log = kp.ListProperty()  # ^
    ftp_sessions = kp.DictProperty()  # Each individual FTP connection is stored here

    def __init__(self):
        super().__init__()
        self.email = EmailNotify()

        #x Retrieve iNews FTP credentials and IP from external source
        with open("C:\\Program Files\\RundownReader_Server\\xyz\\aws_creds.json") as aws_creds:
        #with open("/Users/joseedwa/PycharmProjects/xyz/aws_creds.json") as aws_creds:  # Move these credentials
            inews_details = json.load(aws_creds)
        self.ip = inews_details[1]['ip']
        self.passwd = inews_details[1]['passwd']
        self.user = inews_details[1]['user']

        self.dow = datetime.today().strftime('%a').lower()  # Set Day Of the Week to be used by automation day switching

        Clock.schedule_interval(self.activity_monitor, 61)
        self.init_log_aws_upload_schedule = Clock.schedule_interval(self.init_aws_log_upload, 60)

        self.connection_limit = 5  # Limited amount of connections to server. Added security
        self.connection_amount = 0

        # interrogated when the countdown ends to see if the process should repeat
        self.manual_switches = {'gb': False,
                                'lk': False,
                                'tm': False,
                                'lw': False,
                                'cs': False}

        # and if that process should abide by the automation schedule
        self.automation_switches = {'gb': False,
                                    'lk': False,
                                    'tm': False,
                                    'lw': False,
                                    'cs': False}

        with open('schedule.json') as sched:  # Amendable schedule saved in ext .json to survive app restarts
            self.schedule = json.load(sched)

        with open('log.txt', 'r') as f_in:
            data = f_in.read().splitlines(True)
        with open('log.txt', 'w') as f_out:
            f_out.writelines(data[-3000:])

        # Empty \stories subdirectories of any remnant downloads
        for parent, dirnames, filenames in \
                os.walk("C:\\Users\\Avid.MSTRO-SPR-WW\\PycharmProjects\\RRS_New\\RundownReader_Server_NEW\\stories"):
            for fn in filenames:
                os.remove(os.path.join(parent, fn))

    def build(self):
        """
        Called as the .kv elements are being constructed, post init()
        """
        self.title = 'RRS'
        InewsPullSortPush.app = self  # how alternate .py files communicate back to the main App class

        # Populate each automation schedule input box. The boxes follow the same layout as schedule.json so both
        # can be 'zipped' through concurrently, correctly transferring data between the two locations

        # for prod in self.root.ids.settings_screen.ids:
        #     if 'auto' in prod:
        #         for input_box in prod:
        #             input_box.text = self.schedule[prod[-2:]][input_box]

        # self.root.ids.main_screen.ids['lk_auto_switch'].active = True
        # self.root.ids.main_screen.ids['lw_auto_switch'].active = True

    def init_aws_log_upload(self, dt=None):
        current_minute = int(str(datetime.now())[14:16])
        if current_minute in [0, 15, 30, 45]:
            self.init_log_aws_upload_schedule.cancel()
            Clock.schedule_interval(self.aws_log_upload, 900)

    def aws_log_upload(self, dt=None):
        self.connection_amount = 0  # Hijack this 15 min periodical method to reset connection_caps
        with open('log.txt', 'r') as f_in:
            data = f_in.read().splitlines(True)
        with open('log_for_aws.txt', 'w') as f_out:
            f_out.writelines(data[-50:])
        print('sending log to aws')
        upload_to_aws('log_for_aws.txt', 'rundowns', 'log.txt')

    def activity_monitor(self, dt=None):
        """
        An extra safety net frequently checking the log to make sure that if a switch is on, it is updating the log
        regularly. If there is a freeze in any process the log will stop updating and this process will trigger an
        email alert
        Loop through dict of active sessions, if on and True compare the last logged minute against the current minute,
        calculate the difference between the two and if greater than 1 minute send warning email
        """
        current_minute = int(str(datetime.now())[14:16])

        for prod, active in self.manual_switches.items():
            if active:
                log = getattr(self, prod + '_log')
                last_log_minute = int(log[-1][3:5])
                difference = current_minute - last_log_minute

                if difference not in [0, 1, -59]:  # -59 in list of acceptable differences because top of the hour minus
                    # the previous minute of 59 = -59
                    print('!!!!!!????? HAS A PROCESS FROZEN????!!!!!')
                    self.email.email_error_notification()
                    # if this works send different tier email

    def update_schedule(self):
        """
        Precise reverse of the last loop in build(). schedule.json updated when the schedule is manually changed
        """
        for prod in self.root.ids:
            if 'auto' in prod:
                for input_box in self.root.ids[prod].ids:
                    self.schedule[prod[-2:]][input_box] = self.root.ids[prod].ids[input_box].text

        with open('schedule.json', 'w') as f:
            f.write(json.dumps(self.schedule, indent=4))

    def automate(self, activity, prod):
        """
        Fired by the automate switch, it triggers the relevant day switch and in turn starts all other processes
        :param activity:  The switches bool state
        :param prod: 'gb', 'lk', etc
        :return:
        """
        try:
            self.root.ids.main_screen.ids[prod + self.dow + '_switch'].active = activity
        except KeyError:
            print('its the weekend yall')
            self.root.ids.main_screen.ids[prod + '_fri_switch'].active = activity

    def inews_connect(self, local_dir, export_path, color):
        """
        Establish an FTP connection to the iNews server.
        Firstly a connection error_limit is consulted to prevent flooding the server in error.
        Then it will close any open conns for that production before using login credentials to establish an FTP login
        The login remains open until it gets switched off in the UI
        :param local_dir: local dir used to temporarily house the FTP downloads
        :param export_path: the AWS dir
        :param color: used to color output console text correctly
        :return:
        """
        prod = export_path[3:5]  # = gb / lk / tm / lw / cs
        self.connection_amount += 1
        if self.connection_amount <= self.connection_limit:
            try:  # If session exists, quit
                self.ftp_sessions[prod].quit()
                print(str(datetime.now()) + 'Closing connection: ' + str(self.ftp_sessions[prod]))
            except (KeyError, AttributeError):
                pass  # Key doesn't exist before first run-through

            try:
                self.ftp_sessions[prod] = FTP(self.ip)  # Start a new FTP session and store it as an object in a dict
                self.ftp_sessions[prod].login(user=self.user, passwd=self.passwd)  # Login to FTP
                self.console_log(local_dir[8:], color + "Opening new FTP connection.[/color]")
                print(str(datetime.now())[:19] + ' ~ Opening new FTP connection for ' + prod.upper() + ': ' +
                      str(self.ftp_sessions[prod]))
            except OSError:  # Handles network disconnect error
                print(str(datetime.now()) + 'Network is unreachable. Check internet connection')
                return self.console_log(local_dir[8:], color + "Network is unreachable. Check internet connection")
        else:
            self.console_log(local_dir[8:], color + "Conn limit reached. Restart software.[/color]")
            print(str(datetime.now())[:19] + ' ~ 5/5 connections per 15 min exceeded. Wait 15 min or restart software')
            self.email.email_error_notification()
            self.root.ids.main_screen.ids[prod + '_auto_switch'].active = False

    def inews_disconnect(self, local_dir, export_path, color):
        """
        Gracefully disconnect the FTP link to the iNews server.
        """
        prod = export_path[3:5]  # = gb / lk / tm / lw / cs

        # When closing the App from the UI 'killall' is sent to this method to close any open connections
        if prod == 'l':
            for sesh in self.ftp_sessions.values():
                sesh.quit()
                print(str(sesh) + ': connection has been closed')

        else:  # If export_path != 'killall', close specific rundown conn
            self.ftp_sessions[prod].quit()
            print(str(datetime.now())[:19] + ' ~ Closing FTP connection for ' + prod.upper() + ': ' +
                  str(self.ftp_sessions[prod]))
            del self.ftp_sessions[prod]
            self.console_log(local_dir[8:], color + "Terminated. Closing FTP conn.[/color]")

    def rundown_switch(self, switch, rundown, local_dir, export_path, color):
        """
        Called when the switch in the 'START' column is toggled to start/stop the sequence of methods
        It lets repeat_switches know if the rundown download-sort-upload, should be repeated at the end of the countdown
        starts new FTP session and triggers the next function in the sequence

        note:~ These params are passed down through each method
        :param switch: bool value passed by the state of the switch that triggers this method
        :param rundown: iNews rundown dir path

        """
        prod = export_path[3:5]  # = gb / lk / tm / lw / cs
        self.manual_switches[prod] = switch  # Update dict used at the end of the countdown repeat - yes/no

        if switch:  # If value from the switch being toggled is True/active/on
            self.inews_connect(local_dir, export_path, color)
            self.collect_rundown_thread(rundown, local_dir, export_path, color)  # Begin next step on new thread

        else:  # If switch is turned off. Close FTP conn gracefully and remove it from the ftp_sessions dict
            self.inews_disconnect(local_dir, export_path, color)
            self.root.ids.main_screen.ids[prod + '_auto_switch'].active = False

    def collect_rundown_thread(self, rundown, local_dir, export_path, color):
        """
            Threads are used in Kivy to prevent graphical locking of the interface
        """
        if self.manual_switches[export_path[3:5]]:
            t = Thread(target=self.collect_rundown, args=(rundown, local_dir, export_path, color))
            t.daemon = True
            t.start()

    def collect_rundown(self, rundown, local_dir, export_path, color):
        """
        Create a new instance of the iNews process class and initialise the sequence of methods within. See
        inews_pull_sort_push.py for details.
        Compare the results of the latest iNews download against the previous file, if identical, skip AWS upload.
        If the new file is empty then skip the upload, this is just so the App doesn't show an empty screen
        If neither of these statements are true then proceed to upload to AWS via a new thread
        """

        with open('exports/sv/' + export_path + '.json') as file:
            last_export = json.load(file)  # Compare last file...

        inews_conn = InewsPullSortPush()
        inews_conn.init_process(rundown, local_dir, export_path, color)  # ...(update file)...

        with open('exports/sv/' + export_path + '.json') as file:
            new_export = json.load(file)  #...against the new file

        if last_export == new_export:  # If they match, skip AWS and proceed to countdown
            self.console_log(local_dir[8:], color + "File identical. No upload.[/color]")
            self.countdown(self.determine_frequency(local_dir[8:10]), rundown, local_dir, export_path, color)

        else:  # Otherwise proceed to AWS upload via new Thread
            t = Thread(target=self.push_to_aws, args=(rundown, local_dir, export_path, color))
            t.daemon = False
            t.start()

    def push_to_aws(self, rundown, local_dir, export_path, color):
        """
        Once new pv/sv export files have been created, push to AWS (see s3_connection.py)
        establish how long the next countdown() should take and run it
        """
        if self.manual_switches[export_path[3:5]]:

            upload_to_aws('exports/pv/' + export_path + '.json', 'rundowns', 'pv/' + export_path)
            upload_to_aws('exports/sv/' + export_path + '.json', 'rundowns', 'sv/' + export_path)

            duration = self.determine_frequency(local_dir[8:10])
            self.console_log(local_dir[8:], color + "Uploading json files to AWS[/color]")
            self.countdown(duration, rundown, local_dir, export_path, color)

    def determine_frequency(self, filename):
        """
        Access the string values (hh:mm) of self.schedule and determine if the current time ((int) seconds from
        midnight) falls between any schedule times. If so, return the return duration associated with the schedule
        period to be used by the next countdown()
        :param filename: received as a slice of the local_dir to extract an identifier, e.g. 'lk', 'tm'
        """
        now = datetime.now()  # determine current seconds from midnight in integer form
        seconds_midnight = int((now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds())

        start_h = 0
        start_m = 0
        fin_h = 0
        fin_m = 0

        if self.automation_switches[filename]:  # If automate switch for the selected production is on:
            for x, y in self.schedule[filename].items():  # Loop dict for each key(x), val(y)
                if 'start' in x:
                    start_h, start_m = (int(x) for x in y.split(':'))  # Store start hour and min
                elif 'fin' in x:
                    fin_h, fin_m = (int(x) for x in y.split(':'))  # Store finish hour and min
                else:
                    freq = int(y)  # Lastly store it's freq, then before moving onto the next start item check if
                    # the current actual time falls between the saved start and finish times:
                    if ((start_h * 3600) + (start_m * 60)) < seconds_midnight < ((fin_h * 3600) + (fin_m * 60)):
                        return freq  # If it does then return the set frequency for that time period and break out loop

            return 3651  # If current actual time fail to fall within any schedules then set default freq to 1 hour

        else:
            # If automate switch is off, return default
            return 30  # move this to options eventually

    def countdown(self, duration, rundown, local_dir, export_path, color):
        """
        The last function in the sequence. Using the duration parameter countdown once a second using schedule_once(1)
        When duration == 0 and if the switch has remained on, repeat the entire process
        :param duration: time remaining
        """
        prod = export_path[3:5]

        if duration == 0 and self.manual_switches[prod]:
            return self.check_dow_and_proceed(rundown, local_dir, export_path, color)
            # return self.collect_rundown_thread(rundown, local_dir, export_path, color)

        duration -= 1
        if self.manual_switches[prod]:  # If start switch hasn't been deactivated
            Clock.schedule_once(lambda dt: self.countdown(duration, rundown, local_dir, export_path, color), 1)
            # Update console every second
            self.console_log(local_dir[8:], color + "Repeating  process in " + str(duration) + "[/color]")

    def check_dow_and_proceed(self, rundown, local_dir, export_path, color):
        """
        Once the countdown has ended, and jsut before repeating the process, check to see if the Day Of the Week has
        changed since the last run-through.
        :param current_dow: get dow in shortened string, e.g. 'mon', 'thu'
        """
        prod = export_path[3:5]
        current_dow = datetime.today().strftime('%a').lower()

        if prod == 'lk':  # LK doesn't adhere to DOW rules, break out early
            return self.collect_rundown_thread(rundown, local_dir, export_path, color)

        if self.dow == current_dow:  # If the day has not changed since the last pull, repeat as normal
            return self.collect_rundown_thread(rundown, local_dir, export_path, color)

        elif current_dow == 'sat' or current_dow == 'sun':  # If weekend, check every hour for change until 'mon'
            time.sleep(3600)
            return self.check_dow_and_proceed(rundown, local_dir, export_path, color)

        else:  # If the hour has passed midnight and entered a new day since the last pull
            self.root.ids.main_screen.ids[prod + '_' + self.dow + '_switch'].active = False  # Turn off yesterday switch
            self.dow = current_dow  # Update Day Of Week
            self.root.ids.main_screen.ids[prod + '_auto_switch'].active = True  # Re-trigger the auto switch to proceed



    def console_log(self, filename, text):
        """
        Update the output console and update log.txt
        :param text: new line of text to display
        :return:
        """
        message = ''  # create var

        time = str(datetime.now())[11:19]
        log = getattr(self, filename[:2] + '_log')  # define log/list to update

        # Refine the log to not repeat countdown lines, replace the last one instead
        if 'pulled' in text or 'Repeating' in text:
            # Don't delete previous lines containing:
            if 'download' not in log[-1] and 'identical' not in log[-1] \
                    and 'empty' not in log[-1] and 'AWS' not in log[-1]:
                log.pop(-1)

        log.append(str(time) + ': ' + text)  # add text and time stamp to log

        if 'pulled' not in text:
            with open("log.txt", "a") as logfile:
                logfile.write(str(datetime.now())[:-7] + ' ' + filename[:2].upper() + ' ' + text[14:-8] + '\n')

        # Calculate how many lines should be displayed dependant on the height of the console window
        console_height = (self.root.ids.main_screen.ids['console_' + filename[:2]].height / 16)

        if len(log) > console_height:  # keep the log fewer than n rows
            log.pop(0)

        for i in log:  # build output
            message += i + '\n'

        self.root.ids.main_screen.ids['console_' + filename[:2]].text = message  # update console

    def on_stop(self):
        """
        Run when the app is closed gracefully
        """
        self.inews_disconnect('local_dir', 'kill', '0123456')
        print('cya')


class AutoTI(TextInput):

    pat = re.compile('[^0-9]')

    def insert_text(self, substring, from_undo=False):
        pat = self.pat

        if not from_undo and len(self.text) > 4:
            s = substring
            return s
        elif len(self.text) == 1:
            s = re.sub(pat, '', substring) + ':'
        else:
            s = re.sub(pat, '', substring)

        return super(AutoTI, self).insert_text(s, from_undo=from_undo)


if __name__ == '__main__':
    ConsoleApp().run()
