# Property Of The British Broadcasting Corporation 2020 - Oliver Mardle.
from ftplib import FTP
import re
import datetime
import json
import os
import time


class InewsPullSortSaveLK:
    app = None
    epoch_time = int(time.time())
    data = []
    story_ids = []

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

    def set_backtimes(self):
        """
        Looping the the list of dicts, self.data, this process looks out for timing related key/values and gives each
        dict a new key, ['backtime'] with a calculated value of when the story goes on air. The observed key/values are:
        'back-time uec' = Infrequent hard-out, unchangeable timings for the show. e.g. start/end of show, opt
        'air-date' = If the show deviates from pre-set times, the PA manually sets this time. This takes precedent.
        'total-time uec' = The total-time/duration of the story

        note:~ iNews appends ' uec' to back-time and total-time keys IF they have values.

        Each loop looks for 'air-date' first, 'back-time' second, 'total-time' last.

        If air-date or back-time is found assign its value to the new key ['backtime'] and to the variable current_time
        If they also have 'total-time' then the variable next_time = current_time + total-time
        After this break out of the current loop iteration and begin the next

        If no air-date or back-time exists but a total-time does, then proceed to give dict['backtime'] the previously
        forecast next_time value.

        note:~ We have to work with current_time amd next_time vars in this way because just working with say
        ['backtime'] and total-time on the same story/dict would always result in calculating a backtime in the future

        Lastly, if no timing related value are found in the dict, dict['backtime'] = the previously set current_time
        """

        current_time = 0  # Used to add a value to the dict['backtime'] IF its duration/total-time == 00:00
        next_time = 0  # IF a duration/total-time IS detected, that value is added to current_time to forecast when the
        # current item will end and the next one begins

        for d in self.data:
            try:
                # 'air-date' comes in as epoch. converted to str(HH:MM:SS) sliced in to hrs, min, secs and converted
                # back to int to calculate seconds from midnight...
                air_hour = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[11:13])
                air_min = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[14:16])
                air_secs = int(str(datetime.datetime.fromtimestamp(int(d['air-date'])))[17:19])

                current_time = d['backtime'] = (air_hour * 3600) + (air_min * 60) + air_secs
                next_time = current_time + int(d['total-time uec'])
                continue

            except (KeyError, ValueError):  # Key: 'total-time uec' not in every dict, Val: air-date
                pass

            try:
                current_time = d['backtime'] = int(d['back-time uec'][1:])

                if 'total-time uec' in d:
                    next_time = current_time + int(d['total-time uec'])
                continue

            except KeyError:  # try only works if 'back-time uec' present
                pass

            if 'total-time uec' in d:
                d['backtime'] = next_time
                next_time += int(d['total-time uec'])
            else:
                d['backtime'] = current_time

            # print(d['page-number'], d['title'], str(datetime.timedelta(seconds=d['backtime'])))


inews = InewsPullSortSaveLK()

# inews.pull_xml_via_ftp("*TM.*OUTPUT.RUNORDERS.FRIDAY.RUNORDER", "stories/tm/fri/")
# inews.convert_xml_to_dict("stories/tm/fri/")

inews.pull_xml_via_ftp("*LW.RUNORDERS.MONDAY", "stories/lw/mon/")
inews.convert_xml_to_dict("stories/lw/mon/")

inews.set_backtimes()
