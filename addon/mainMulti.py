import time
import os
import json
import sys
import threading
import traceback
import logging
import paho.mqtt.client as mqtt

from enum import Enum
from collections import defaultdict

from pytcs_tecnoalarm import TCSSession
from pytcs_tecnoalarm.api_models import ZoneStatusEnum

mqtt_host = os.getenv("MQTT_HOST", "localhost")
mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
mqtt_username = os.getenv("MQTT_USERNAME")
mqtt_password = os.getenv("MQTT_PASSWORD")
mqtt_qos = int(os.getenv("MQTT_QOS", 2))
mqtt_retain = os.getenv("MQTT_RETAIN", "True").lower() == "true"

tcs_username = os.getenv("TCS_USERNAME")
tcs_password = os.getenv("TCS_PASSWORD")
tcs_serial = os.getenv("TCS_SERIAL")
tcs_can_arm = os.getenv("TCS_CAN_ARM", "True").lower() == "true"
tcs_can_disarm = os.getenv("TCS_CAN_DISARM", "True").lower() == "true"

secret_file = '/data/tcsSession.json'
refresh_period = 2

mqtt_topic_base = "tecnoalarm"
mqtt_topic_zone = "zones"
mqtt_topic_program = "programs"

zones = defaultdict(dict)
programs = defaultdict(dict)

centrale = None
session = None
mqttClient = None

class ProgramStatusEnum(str, Enum):
	UNLOCKED = "UNLOCKED"
	UNLOCKING = "UNLOCKING"
	LOCKING = "LOCKING"
	LOCKED = "LOCKED"
	
def int_to_enum(val):
	if val == 0:
		return ProgramStatusEnum.UNLOCKED
	elif val == 1:
		return ProgramStatusEnum.UNLOCKING
	elif val == 2:
		return ProgramStatusEnum.LOCKING
	elif val == 3:
		return ProgramStatusEnum.LOCKED
	else:
		return None


def on_connect(client, userdata, flags, reason_code, properties):
	logger.info('Connected to MQTT Home Assistant frontend with result code:' + str(reason_code))
	for progId, progData in programs.items():
		topic = "{}/{}/{}/set".format(mqtt_topic_base, mqtt_topic_program, progData['name'])
		res, mid = client.subscribe(topic)
		logger.info('Subscribing to ' + topic + ': result(' + str(res) + ') id(' + str(mid) + ')')
	
def on_message(client, userdata, msg):
	message = msg.payload.decode("utf-8")
	logger.info('Received on topic[' + msg.topic + ']: ' + message)
	try:
		target = msg.topic.split("/")[-2]
		request = json.loads(message)
		for progId, progData in programs.items():
			if progData['name'] == target:
				if request['command'] == 'LOCK' and progData['status'] != 3:
					if tcs_can_arm:
						session.enable_program(progId)
					else:
						logger.warning('Can not arm')
				elif request['command'] == 'UNLOCK' and progData['status'] != 0:
					if tcs_can_disarm:
						session.disable_program(progId)
					else:
						logger.warning('Can not disarm')
	except AttributeError:
		logger.warning('Should work!!')
	except ValueError:
		logger.error('Wrong message received:' + message)
	except AssertionError:
		logger.error('Invalid command')			

def on_subscribe(client, userdata, mid, reason_code_list, properties):
	logger.info('Subribed to topic ' + str(mid))
	if reason_code_list[0].is_failure:
		logger.error('Broker rejected you subscription: ' + str(reason_code_list[0]))
	else:
		logger.info('Broker granted the following QoS: ' + str(reason_code_list[0].value))

def on_unsubscribe(client, userdata, mid, reason_code_list, properties):
	if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
		logger.info('Unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)')
	else:
		logger.error('Broker replied with failure: ' + str(reason_code_list[0]))
		client.disconnect()

def clean_name(str):
	return str.replace(" ", "_").replace(".", "_").replace("-", "_").lower()

def init_tecnoalarm(max_retry):
	retry = 0
	global session
	global centrale
	logger.info('Init tecnoalarm API...')
	tcs_session = {}
	tcs_token = tcs_session.get('token', None)
	tcs_appid = tcs_session.get('appid', None)
	try:
		f = open(secret_file, 'r')
		tcs_session = json.loads(f.read())
		tcs_token = tcs_session['token']
		tcs_appid = tcs_session['appid']
		f.close()
	except Exception as e:
		logging.error(traceback.format_exc())
	
	initOk = False
	while retry < max_retry:
		try:
			logger.info('Try to connect to server (' + str(retry+1) + '/'+str(max_retry) + ')')
			if tcs_token and tcs_appid:
				logger.info('LOGGING WITH TOKEN')
				session = TCSSession(tcs_token, tcs_appid)
			else:
				logger.info('LOGGING WITH CREDENTIAL')
				session = TCSSession()
				session.login(tcs_username, tcs_password)
				tcsLogin = {'appid': str(session.appid), 'token': session.token}
				f = open(secret_file, 'w')
				f.write(json.dumps(tcsLogin))
				f.close()
			session.get_centrali()
			centrale = session.centrali[tcs_serial]
			session.select_centrale(centrale.tp)
			retry = max_retry
			logger.info('Connection established')
			initOk = True
		except AssertionError:
			logger.warning('Connection error. Wait 10 seconds and retry')
			retry = retry+1
			time.sleep(11)
		except Exception as e:
			logging.error(traceback.format_exc())

	if initOk:
		logger.info('Init tecnoalarm API...DONE')
	else:
		logger.error('Init tecnoalarm API...FAILED')
	return initOk

def init_mqtt():
	global mqttClient
	logger.info('Start creating MQTT client...')
	mqttClient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
	mqttClient.on_connect = on_connect
	mqttClient.on_message = on_message
	mqttClient.on_subscribe = on_subscribe
	mqttClient.on_unsubscribe = on_unsubscribe
	mqttClient.username_pw_set(mqtt_username, mqtt_password)
	mqttClient.connect(mqtt_host, mqtt_port, 60)
	logger.info('Start creating MQTT client...DONE') 

def refresh_zones():
	logger.debug('Refresh zones')
	try:
		z = session.get_zones()
		if z:
			new_zones = []
			for zone in z.root:
				if zone.status == ZoneStatusEnum.UNKNOWN or not zone.allocated:
					continue
				if zones[zone.idx]['status'] != zone.status:	
					new_zones.append(zone.idx)
				zones[zone.idx]['status'] = zone.status
				zones[zone.idx]['available'] = 'online' if zone.status != ZoneStatusEnum.ISOLATED else 'offline'
			if new_zones:
				updateZoneThread = threading.Thread(target=update_zones, args=(new_zones,))
				updateZoneThread.daemon = True
				updateZoneThread.start()
	except Exception as e:
		logging.error(traceback.format_exc())
		logger.warning('Failed to get zones (Generic error):' + str(e))

	threading.Timer(refresh_period, refresh_zones).start() 

def refresh_programs():
	logger.debug('Refresh programs')
	try:
		p = session.get_programs()
		if p:
			new_programs = []
			for programstatus, programdata in zip(p.root, centrale.tp.status.programs):
				if len(programdata.zones) == 0:
					continue
				if programs[programdata.idx]['status'] != int_to_enum(programstatus.status):	
					new_programs.append(programdata.idx)
				programs[programdata.idx]['status'] = int_to_enum(programstatus.status)
				if programs[programdata.idx]['status'] == ProgramStatusEnum.LOCKING or programs[programdata.idx]['status'] == ProgramStatusEnum.LOCKED:
					programs[programdata.idx]['available'] = tcs_can_disarm
				elif programs[programdata.idx]['status'] == ProgramStatusEnum.UNLOCKING or programs[programdata.idx]['status'] == ProgramStatusEnum.UNLOCKED:
					programs[programdata.idx]['available'] = tcs_can_arm
				else:
					programs[programdata.idx]['available'] = False
			if new_programs:
				updateProgramThread = threading.Thread(target=update_programs, args=(new_programs,))
				updateProgramThread.daemon = True
				updateProgramThread.start()
	except Exception as e:
		logging.error(traceback.format_exc())
		logger.warning('Failed to get programs (Generic error):' + str(e))
	
	
	threading.Timer(refresh_period, refresh_programs).start() 
	
def update_zones(data):
	logger.info('Update zones: ' + str(data))
	for z in data:
		topic = "{}/{}/{}".format(mqtt_topic_base, mqtt_topic_zone, zones[z]['name'])
		message = json.dumps(zones[z])
		logger.debug(message)
		res = mqttClient.publish(topic, message, mqtt_qos, mqtt_retain)
		if res:
			logger.debug("Update zones sent")
		else:
			logger.warning("Failed to sent update zones")

def update_programs(data):
	logger.info('Update programs: ' + str(data))
	for p in data:
		topic = "{}/{}/{}".format(mqtt_topic_base, mqtt_topic_program, programs[p]['name'])
		message = json.dumps(programs[p])
		logger.debug(message)
		res = mqttClient.publish(topic, message, mqtt_qos, mqtt_retain)
		if res:
			logger.debug("Update programs sent")
		else:
			logger.warning("Failed to sent update programs")

def init_zones():
	logger.info('Init zones')
	z = session.get_zones()
	for zone in z.root:
		if zone.status == ZoneStatusEnum.UNKNOWN or not zone.allocated:
			continue
		zones[zone.idx]['description'] = zone.description,
		zones[zone.idx]['name'] =  clean_name(zone.description)
		zones[zone.idx]['status'] =  None
		zones[zone.idx]['available'] = False

def init_programs():
	logger.info('Init programs')
	p = session.get_programs()
	for programstatus, programdata in zip(p.root, centrale.tp.status.programs):
		if len(programdata.zones) == 0:
			continue
		programs[programdata.idx]['description'] = programdata.description,
		programs[programdata.idx]['name'] =  clean_name(programdata.description)
		programs[programdata.idx]['zones'] = programdata.zones
		programs[programdata.idx]['status'] =  None
		programs[programdata.idx]['available'] = False

logger = logging.getLogger("")
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)-5.5s] %(message)s",
    handlers=[
        #logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
	while True:
		if init_tecnoalarm(10):
			init_zones()
			init_programs()
			init_mqtt()

			threading.Timer(refresh_period, refresh_zones).start() 
			threading.Timer(refresh_period, refresh_programs).start() 

			logger.info('Start main loop addon...')

			mqttClient.loop_forever()
		else:
			logger.error('Failed to start tecnoalarm. Delete secrets...')
			try:
				os.remove(secret_file)
				logger.error('Failed to start tecnoalarm. Delete secrets...DONE')
			except OSError as e:
				logger.warning('Failed to delete secret file (' + e.filename + '). Error:' + e.strerror)
	
		time.sleep(10)