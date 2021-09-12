# Property Of The British Broadcasting Corporation 2020 - Oliver Mardle.
from ftplib import FTP
import re
import datetime
import json
import os
import time


class InewsPullSortSaveLK:
    app = None
    console = None
    epoch_time = int(time.time())
    timeout_counter = 0

    def __init__(self):
        self.story_ids = []  # list of story_id titles in string form. E.g. 'E45RF34'
        self.data = []  # Once converted from XML each story dict gets stored in here
        self.times_list = []  # total time/duration for each story in valid MM:SS, 00:00 if blank
        self.hard_backtimes = []  # hard-out backtimes from iNews in seconds format. E.g. @34256
        self.backtime_pos_list = []  # An indexed list for the above hard-out backtimes, blank positions if no backtime present
        self.backtimes_calculated = []  # Final backtime list. Appended in reverse by subtracting time_list elements from hard_backtimes lst

    def pull_xml_via_ftp(self, inews_path, local_dir):
        counter = 0
        with open("/Users/joseedwa/PycharmProjects/xyz/aws_creds.json") as aws_creds:
        #with open("C:\\Program Files\\RundownReader_Server\\xyz\\aws_creds.json") as aws_creds:
            inews_details = json.load(aws_creds)
            user = inews_details[1]['user']
            passwd = inews_details[1]['passwd']
            ip = inews_details[1]['ip']

        # Open FTP connection
        ftp = FTP(ip)
        ftp.login(user=user, passwd=passwd)

        # Retrieve rundown using 'path' parameter passed when function is called
        # e.g. ftp.cwd("CTS.TX.0600")
        ftp.cwd(inews_path)

        # Store story ID as list of titles. E.g. '5AE4RT2'
        self.story_ids = ftp.nlst()

        # Cycles through each line/Story ID and opens as a new file
        # RETRIEVE the contents of each Story ID from iNews and store it in new_story_file hen save to local_dir

        for story_id_title in self.story_ids:
            self.epoch_time = int(time.time())

            with open(local_dir + story_id_title, "wb") as new_story_file:
                ftp.retrbinary("RETR " + story_id_title, new_story_file.write)

            new_story_file.close()
            counter += 1
            self.epoch_time = int(time.time())
            if counter % 25 == 0:
                print(str(counter))
            #     self.app.console_log(filename, color + str(counter) + ' stories pulled[/color]')

        # Close FTP connection
        ftp.quit()

    def convert_xml_to_dict(self, local_dir):
        # TODO: NOTE - focus, brk, and floated default false values removed, try and work App without these
        """ A bulky method that in summary, takes each raw FTP NSML story file and converts it to a dict.
        Stripping out the bits we don't need and reformatting some dict key/values along the way.
        By the end we are left with a neat list of dicts (self.data)
        NSML (XML) Example:
        <nsml version="-//AVID//DTD NSML 1.0//EN">
        <head>
        3<meta rate=180 float>
        <rgroup number=46></rgroup>
        <wgroup number=18></wgroup>
        <formname>THISMORNINGS_V2</formname>
        7<storyid>259847d2:00ce0ba3:605b6fc5</storyid>
        </head>
        <story>
        <fields>
        <f id=source></f>
        <f id=var-script></f>
        <f id=camera></f>
        <f id=location></f>
        <f id=page-number></f>
        <f id=title>+ PERMA &amp; HASHTAG</f>
        <f id=video-id></f>
        <f id=event-status></f>
        <f id=item-channel></f>
        <f id=sound></f>
        <f id=audio-time>0</f>
        <f id=runs-time>0</f>
        <f id=total-time>0</f>
        <f id=back-time></f>
        <f id=modify-date>1616605125</f>
        <f id=modify-by>barrtho1</f>
        <f id=air-date></f>
        <f id=var-1></f>
        <f id=gfxready></f>
        <f id=gfxprep></f>
        <f id=vartm-01></f>
        </fields>
        We only want some meta data from line 3, story_id form line 7 and then everything between /fields.
        """
        tp = 0

        # Cycle through and open each newly created story file
        for story_id_title in self.story_ids:
            break_out_flag = False
            with open(local_dir + story_id_title, "rb") as story_file:

                # File will now have relevant contents stripped, sanitised and placed into this dict:
                story_dict = {}

                # We only want the data between XML stages <fields> and </fields>. Copy is set to false outside of that
                copy = False

                # Loop through each line in the XML story file and extract necessary info into dict
                for line in story_file:
                    # Checks if 'float' is in 'meta' line of story at the top of the file. Plus it decodes line from
                    # bytes and strips off any new line characters that may be present
                    # TODO: refactor these float and break keys to not have elif/False outcomes
                    if "float" in (line.decode()).strip() and "<meta" in (line.decode()).strip():
                        # If True it adds 'floated' key to 'storyLine' dictionary and sets its value it value to True
                        break_out_flag = True
                        break

                    # Check if 'break' is in 'meta' line of story.
                    if "break" in (line.decode()).strip() and "<meta" in (line.decode()).strip():
                        story_dict["brk"] = True
                        # OMIT?
                    elif "break" not in (line.decode()).strip() and "<meta" in (line.decode()).strip():
                        # False if not
                        story_dict["brk"] = False

                    # Extract the first part of story id to be used as a unique ID in the app
                    if "<storyid>" in (line.decode()).strip():
                        result = re.search('<storyid>(.*)</storyid>', (line.decode()).strip())
                        sep = ':'
                        stripped = result.group(1).split(sep, 1)[0]

                        story_dict["story_id"] = stripped

                    if (line.decode()).strip() == "<fields>":
                        # If found, copy = True. Copy controls whats added to the storyLine dictionary. In this case
                        # Everything between "<fields>", line by line until we reach "</fields>"
                        copy = True
                        # OMIT?
                        continue

                    elif (line.decode()).strip() == "</fields>":
                        copy = False
                        continue

                    # Depending on what is decided above, the line is cleaned and copied - or ignored
                    if copy:

                        # Decode line from bytes and strips off any new line characters
                        decoded_line = (line.decode()).strip()

                        # New variable which is a regular expression used to pull ID & data from decoded_line.
                        # Simply put, it's making 2 groups. Group#1 is the string between 'id=' and '>', group#2 between
                        # '>' and '</f>'
                        entry = re.search("<f id=(.*)>(.*)</f>", decoded_line)

                        # These become key value pairs for 'story_dict'
                        key = entry.group(1)
                        value = entry.group(2)

                        if 'amp;' in value:
                            value = value.replace('amp;', '')

                        if 'gt;' in value:
                            value = value.replace('gt;', '')

                        # Else write key and value as they are
                        story_dict[key] = value
                            # print(key)


                if not break_out_flag:
                    # Append story_dict to 'data' list


                    self.data.append(story_dict)

                # Close story file
                story_file.close()

                # 8) Deletes the file we just read as it's no longer needed
                os.remove(local_dir + story_id_title)

    def set_backtime(self):
        current_time = 0
        next_time = 0
        for d in self.data:
            try:
                # converted a lot due to slice
                air_hour = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[11:13])
                air_min = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[14:16])
                air_secs = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[17:19])
                current_time = d['backtime'] = (air_hour * 3600) + (air_min * 60) + air_secs
                next_time = current_time + int(d['total-time uec'])
                print(d['page-number'], d['title'], str(datetime.timedelta(seconds=d['backtime'])))
                continue
            except (KeyError, ValueError):
                pass

            try:
                current_time = d['backtime'] = int(d['back-time uec'][1:])
                if 'total-time uec' in d:
                    next_time = current_time + int(d['total-time uec'])

                print(d['page-number'], d['title'], str(datetime.timedelta(seconds=d['backtime'])))
                continue
            except KeyError as e:
                pass


            if 'total-time uec' in d:
                d['backtime'] = next_time
                next_time += int(d['total-time uec'])
            else:
                d['backtime'] = current_time


            print(d['page-number'], d['title'], str(datetime.timedelta(seconds=d['backtime'])))






            # if d['air-date'] == "":
            #     try:
            #         d['backtime'] = tp = d['back-time uec'][1:]
            #         print('bt added: ' + str(tp))
            #     except KeyError:
            #         pass
            #
            #
            #     elif d['total-time uec']:
            #         print('attempting total...')
            #         d['backtime'] = tp = tp + d['total-time uec']
            #         print('total updated: ' + str(tp))






                # print(d['backtime'])

                # print('total')
                # print(d['total-time uec'])
                # print('ad')
                # print(d['air-date'])
                # print('uec')
                # print(d['back-time uec'])




inews = InewsPullSortSaveLK()
inews.pull_xml_via_ftp("*TM.*OUTPUT.RUNORDERS.FRIDAY.RUNORDER", "stories/tm/fri/")
inews.convert_xml_to_dict("stories/tm/fri/")
inews.set_backtime()

# if story_dict['airdate'] != "":
#     air_hour = int(str(datetime.datetime.fromtimestamp(int(story_dict['airdate'])))[11:13])
#     air_min = int(str(datetime.datetime.fromtimestamp(int(story_dict['airdate'])))[14:16])
#     air_secs = int(str(datetime.datetime.fromtimestamp(int(story_dict['airdate'])))[17:19])
#     secs_from_m = '@' + str((air_hour * 3600) + (air_min * 60) + air_secs)
#
#     self.hard_backtimes.append(secs_from_m)
#     self.backtime_pos_list.append(secs_from_m)
#     # continue