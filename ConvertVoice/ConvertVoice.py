#!/usr/bin/env python
#   2013-04-25  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   dispose engine to remove cluttered mysql connections
#   2013-04-23  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   sox convert a file if 32 bit depth to 16 bit
#   2013-04-22  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   lessened verbosity of download error
#   2013-04-17  Lawrence Sunglao <lawrence.sunglao@remotestaff.com.au>
#   -   added exception when retrieving file

import settings
import string
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import urllib2
import os
from os.path import basename
import re
from subprocess import call, Popen, PIPE
import shutil
import sys, traceback

from celery.execute import send_task
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


def update_voice_path(userid):
    """
    >>> update_voice_path(69)
    >>> update_voice_path(71599)
    """
    engine = create_engine(settings.MYSQL_DSN)
    voice_path = settings.UPLOADED_MP3 % userid
    sql = text("""UPDATE personal
        SET voice_path = :voice_path
        WHERE userid = :userid""")
    sql_conn = engine.connect()
    sql_conn.execute(sql, userid=userid, voice_path=voice_path)
    sql_conn.close()
    engine.dispose()


def move_mp3_ogg(userid):
    """given the userid, move the file and update personal.voice_path
    >>> move_mp3_ogg(73378)

    >>> move_mp3_ogg(71313)

    """
    if os.path.exists('%s/%s.mp3' % (settings.VOICE_PATH, userid)):
        os.unlink('%s/%s.mp3' % (settings.VOICE_PATH, userid))
    shutil.move('%s.mp3' % userid, settings.VOICE_PATH)

    if os.path.exists('%s/%s.ogg' % (settings.VOICE_PATH, userid)):
        os.unlink('%s/%s.ogg' % (settings.VOICE_PATH, userid))
    shutil.move('%s.ogg' % userid, settings.VOICE_PATH)


def media_to_mp3_ogg(userid, scale, file_name):
    """using mplayer, convert it to wav then convert using lame
    returns a filename for uploading
    >>> media_to_mp3_ogg(69, 1, '69.mp3')
    {'ogg': '69.ogg', 'mp3': '69.mp3'}

    >>> media_to_mp3_ogg(73378, 1, '73378js-1364653587296.flv')
    {'ogg': '73378.ogg', 'mp3': '73378.mp3'}

    >>> media_to_mp3_ogg(71313, 1, '71313js-1361154194706.flv')
    {'ogg': '71313.ogg', 'mp3': '71313.mp3'}

    >>> media_to_mp3_ogg(71056, 1, '71056.wma')
    {'ogg': '71056.ogg', 'mp3': '71056.mp3'}

    >>> media_to_mp3_ogg(42162, 1, '42162.wma')
    {'ogg': '42162.ogg', 'mp3': '42162.mp3'}

    >>> media_to_mp3_ogg(68727, 1, '68727.wav')
    {'ogg': '68727.ogg', 'mp3': '68727.mp3'}

    >>> media_to_mp3_ogg(71797, 1, '71797.wav')
    {'ogg': '71797.ogg', 'mp3': '71797.mp3'}

    >>> media_to_mp3_ogg(72233, 1, '72233.mp3')
    {'ogg': '72233.ogg', 'mp3': '72233.mp3'}

    >>> media_to_mp3_ogg(54663, 1, '54663.flv')
    {'ogg': '54663.ogg', 'mp3': '54663.mp3'}
    """

    #check if audiodump.wav exists, delete it
    if os.path.exists('audiodump.wav'):
        os.unlink('audiodump.wav')

    #dump to audiodump.wav using mplayer
    mplayer_cmd = settings.MPLAYER_DUMP % file_name
    cmd = call(string.split(mplayer_cmd))
    if cmd != 0:
        send_task('notify_devs.send', ['Failed media_to_mp3_ogg function userid:%s with file_name:%s' % (userid, file_name), 'Exit code:%s mplayer_cmd:%s' % (cmd, mplayer_cmd)])
        return

    #normalize audio
    normalize_cmd = settings.NORMALIZE
    cmd = call(string.split(normalize_cmd))
    if cmd != 0:
        send_task('notify_devs.send', ['Failed media_to_mp3_ogg function userid:%s with file_name:%s' % (userid, file_name), 'Exit code:%s normalize_cmd:%s' % (cmd, normalize_cmd)])
        return

    #convert using ogg
    ogg_cmd = settings.OGG_ENCODE % (userid)
    ogg_file = '%s.ogg' % userid
    cmd = call(string.split(ogg_cmd))
    if cmd != 0:
        send_task('notify_devs.send', ['Failed media_to_mp3_ogg function userid:%s with file_name:%s' % (userid, file_name), 'Exit code:%s ogg_cmd:%s' % (cmd, ogg_cmd)])
        return

    #check if audiodump.wav is 32 bits, lame version 3.98.4 doesn't like Bit depth:32 bits
    flag_sox_convert = False
    cmd = Popen(['mediainfo', 'audiodump.wav'], stdout=PIPE)
    out, err = cmd.communicate()
    data = string.split(out, '\n')
    for a in data:
        if re.match('^Bit depth', a) != None:
            if re.search('32 bits', a) != None:
                flag_sox_convert = True

    if flag_sox_convert == True:
        os.rename('audiodump.wav', 'audiosox.wav')
        sox_cmd = settings.SOX_CONVERT % ('audiosox.wav', 'audiodump.wav')
        cmd = call(string.split(sox_cmd))
        if cmd != 0:
            send_task('notify_devs.send', ['Failed media_to_mp3_ogg function userid:%s with file_name:%s' % (userid, file_name), 'Exit code:%s sox_cmd:%s' % (cmd, sox_cmd)])
            return
        os.unlink('audiosox.wav')   #delete file

    #convert using lame
    lame_cmd = settings.LAME_ENCODE % (scale, userid)
    mp3_file = '%s.mp3' % userid
    cmd = call(string.split(lame_cmd))
    if cmd != 0:
        send_task('notify_devs.send', ['Failed media_to_mp3_ogg function userid:%s with file_name:%s' % (userid, file_name), 'Exit code:%s lame_cmd:%s' % (cmd, lame_cmd)])
        return

    #remove dump
    os.unlink('audiodump.wav')

    #remove downloaded file
    if file_name != mp3_file:
        os.unlink(file_name)

    return dict(mp3=mp3_file, ogg=ogg_file)


def download(userid):
    """
    >>> download(69)
    u'69.mp3'

    >>> download(73378)
    u'73378js-1364653587296.flv'

    >>> download(71313)
    u'71313js-1361154194706.flv'

    >>> download(71056)
    u'71056.wma'

    >>> download(42162)
    u'42162.wma'

    >>> download(68727)
    u'68727.wav'

    >>> download(71797)
    u'71797.wav'

    >>> download(72233)
    u'72233.mp3'

    >>> download(54663)
    u'54663.flv'

    >>> #download(73533)
    >>> #download(73513)

    """
    #get voice_path from personal table
    engine = create_engine(settings.MYSQL_DSN)
    sql = text("""SELECT voice_path 
        FROM personal
        WHERE userid = :userid""")
    sql_conn = engine.connect()
    voice_path = sql_conn.execute(sql, userid=userid).fetchone()
    sql_conn.close()
    engine.dispose()

    if voice_path == None:
        send_task('notify_devs.send', ['Failed to download audio of userid:%s ' % (userid), 'No audio file found on personal.voice_path'])
        return

    url_path = voice_path[0]
    if url_path == None:
        send_task('notify_devs.send', ['Failed to download audio of userid:%s ' % (userid), 'No audio file found on personal.voice_path'])
        return
    
    url_path = string.strip(url_path)
    if url_path == '':
        send_task('notify_devs.send', ['Failed to download audio of userid:%s ' % (userid), 'No audio file found on personal.voice_path'])
        return

    if re.match('^http://vps01.remotestaff.biz', url_path) != None:
        file_name = basename(url_path)
    else:
        file_name = basename(url_path)
        if settings.DEBUG:
            url_path = 'http://test.remotestaff.com.au/portal/%s' % url_path
        else:
            url_path = 'https://remotestaff.com.au/portal/%s' % url_path

    try:
        response = urllib2.urlopen(url_path)
        data = response.read()
    except:
        logging.exception('Got exception on download(%s)' % userid)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        send_task('notify_devs.send', ['MQ ConvertVoice.py download(%s) error' % userid, 'url_path:%s' % (url_path)])
        return None

    if len(data) == 0:
        send_task('notify_devs.send', ['Failed to convert audio of userid:%s' % (userid), 'File size is 0 from %s' % url_path])
        return

    f = file(file_name, 'wb')
    f.write(data)
    f.close()
    
    return file_name


if __name__ == '__main__':
    import doctest
    doctest.testmod()
