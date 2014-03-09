import sys
from optparse import OptionParser
from sys import modules
import os.path, time, os
import fnmatch
import os
#from config import folderpaths
import time
import traceback
import string
import re
import gc
import subprocess


folderpaths = [
			'/Users/mg123/Desktop/Mygola-Hiring-2012',
			'/Users/mg123/Desktop/b',
			'/Users/mg123/Desktop',
			'/Users/mg123/Downloads',
			'/Users/mg123/Music'			
		]
SOLR_URL = 'http://localhost:8983/solr'

def filterTxt(data):
	data = data.decode('raw_unicode_escape').replace('\n',' ').replace('\t',' ')
	data = filter(lambda x:  x in string.printable and ord(x) < 128  , data)	
	return data				

def install(field, data, path, length, mode):
	print 'Please download solr from http://lucene.apache.org/solr and unzip it'
	#raw_input('press any key to continue \n')
	print 'open path-to-apache-folder/example/solr/conf/schema.xml'
	#raw_input('press any key to continue \n')
	print 'search for line containing <field name="name" type="text_general" indexed="true" stored="true"/>'
	#raw_input('press any key to continue \n')
	print 'just below to that line add <field name="txt" type="text_general" indexed="true" stored="true"/>'
	#raw_input('press any key to continue \n')
	path = raw_input('Now we need python 2.6.X or 2.7.X, pls provide path to that, it will like "/usr/bin/" \n')
	easy_install_path = os.path.join(path,'easy_install')
	requires = ['solrpy','termcolor']
	for name in requires:
		subprocess.call([easy_install_path, name])
	#raw_input('press any key to continue \n')
	print 'Please download ID3 from http://sourceforge.net/projects/id3-py/ , now unzip it and go to the folder and install it using the cmd'	
	print  os.path.join(path,'python') + " setup.py install"
	raw_input('press any key to continue \n')
		

def refresh(field=None, data=None, path = None, length = None, mode = None):
	print 'refresh'
	return _refresh(field, data, path)

def cronRefresh(field=None, data=None, path = None, length = None, mode = None):
	print 'cronRefresh'
	return _refresh(field, data, path, True)

def search(field, data, path, hlength, mode):
	from termcolor import colored
	from solr import SolrConnection
	#hlength = int(hlength)
	#search solr, get filePath, do a grep and show the line
	#print 'search'
	s = SolrConnection(SOLR_URL)
	if field == 'name':
		query = 'name:"' + data + '"'
		response = s.query(query)
	elif field == 'txt':
		query = 'txt:"' + data + '"'		
		#response = s.query(query, hl=True, hl.q='txt:bandits', hl.fl='txt', hl.fragsize=50, hl.preserveMulti=True, hl.snippets=100)
		if hlength:
			response = s.query(query, fl='id,name', highlight=True, fields = 'txt', hl_q=query, hl_fragsize=hlength, hl_snippets=1000, hl_bs_type = 'SENTENCE')																																	 
		else:
			response = s.query(query, fl='id,name')
	else:
		query = 'name:"' + data + '" OR txt:"' + data + '"'
		#response = s.query(query, hl=True, hl.q='txt:bandits', hl.fl='txt', hl.fragsize=50, hl.preserveMulti=True, hl.snippets=100)
		if hlength:
			response = s.query(query, fl='id,name', highlight=True, fields = 'txt', hl_q=query, hl_fragsize=hlength, hl_snippets=1000, hl_bs_type = 'SENTENCE')
		else:
			response = s.query(query, fl='id,name')

	#print query
	#print response.__dict__
	#print response.highlighting
	if hlength and field != 'name':
		hlength = int(hlength)			
		for id in response.highlighting:
			if os.path.isfile(id):			
				if response.highlighting[id]:
					for txt in response.highlighting[id]['txt']:
						txt = txt.strip()
						startpos = txt.index('<em>')
						endpos = txt.rindex('</em>')
						print (txt[:startpos] + colored(txt[startpos+4:endpos], 'red') + txt[endpos+5:]).replace('<em>', '').replace('</em>', '')
				else:
					fdata = open(id, 'r').read().decode('raw_unicode_escape').replace('\n',' ').replace('\t',' ')
					fdata = filter(lambda x: x in string.printable, fdata)
					for m in re.finditer( data, fdata ):
						start = m.start()-hlength
						if start < 0 :
							start = 0					
						end = m.end() + hlength
						if end > len(fdata):
							end = len(fdata)

						print (fdata[start:m.start()] + colored(fdata[m.start():m.end()], 'red') + fdata[m.end():end]).replace('<em>', '').replace('</em>', '')
				if id.endswith(('.mp3')):
					if mode == 'slow':
						x = raw_input('press `y` to play, `n` to move forward \n')
						if x == 'y':
							subprocess.call(["afplay", id])
				else:					
					print '\t To open the file press cmd + double click '
					print colored("file://"+id, 'blue')			
					print '\n \n'
					if mode == 'slow':
						raw_input('press any key to continue \n')

			else:
				s.delete_query('id:'+id)
	else:
		for hit in response.results:			
			if hit['id']:
				if hit['id'].endswith(('.mp3')):
					if mode == 'slow':
						x = raw_input('press `y` to play, `n` to move forward \n')
						if x == 'y':
							subprocess.call(["afplay", hit['id']])
				else:					
					print '\t To open the file press cmd + double click '
					print colored("file://"+hit['id'], 'blue')			
					print '\n \n'
					if mode == 'slow':
						raw_input('press any key to continue \n')
			else:
				s.delete_query('id:'+hit['id'])





def uninstall(field, data, path, length, mode):	
	from solr import SolrConnection
	s = SolrConnection(SOLR_URL)
	s.delete_query('id:*')
	s.commit()

def _refresh(field=None, data=None, path = None, isCron = None):
	from solr import SolrConnection
	from ID3 import *
	s = SolrConnection(SOLR_URL)
	if path and path != '*':
		#called by user		
		pathsArr = path.split(',')		
	else:
		#called from cron		
		pathsArr = folderpaths
	matches = []
	#handles modify, add
	#deletion will be handled in search when file in solr but not in path
	time.time()
	for path in pathsArr:
		for root, dirnames, filenames in os.walk(path):
			for extension in ['txt', 'log', 'py', 'pl', 'sql', 'mp3']:
				for filename in fnmatch.filter(filenames, '*.' + extension):				
					fullName = os.path.join(root, filename)
					if os.path.getsize(fullName) > 8800000:
						continue
					#print fullName
					if not isCron or (time.time() - os.path.getmtime(fullName) < 24*60*60):				
						try:
							#data = open(fullName, 'r').read().decode('raw_unicode_escape').replace('\n',' ').replace('\t',' ')
							if filename.endswith(('.txt', '.log', '.py', '.pl', '.sql')):								
								data = open(fullName, 'r').read()
								data = filterTxt(data)
							else:								
								audiofile = ID3(fullName)
								audiofilekeys = audiofile.keys()
								if 'TITLE' in audiofilekeys:
									data = audiofile['TITLE'] + " "
								if 'ARTIST' in audiofilekeys:
									data += audiofile['ARTIST'] + " "
								if 'ALBUM' in audiofilekeys:
									data += audiofile['ALBUM'] + " "
								if not data:
									data = ''
								data = data.strip()
							fullName = filterTxt(fullName)
							filename = filterTxt(filename)						
							s.add(id = fullName, name = filename, txt = data)
							s.commit()
						except:																	
							pass
							#print data
							#print traceback.format_exc()
							#print fullName	
							#sys.exit()					
						gc.collect()
	


def process(options):
	command = options.cmd
	if command not in ['install', 'refresh', 'search', 'uninstall', 'cronRefresh']:
		print 'you should have specified one of these cmds install or refresh or search or uninstall, assuming search'
		command = 'search'
	field = options.field
	data = options.data
	if command == 'search' and not data:
		print 'pls specify the txt you want to search'
		return
	path = options.path
	if not path:
		path = '*'	
	length = options.length
	if command == 'search' and length and int(length) > 200:
		print "length greater than 200 not supported, setting it to 50"
		length = 50
	mode = options.mode
	if not mode:
		print "No mode specified, assuming slow"
		mode = 'slow'
	#print command, field, data, path, length
	this_mod = sys.modules[__name__]
	func = getattr(this_mod, command)
	func(field, data, path, length, mode)



if __name__ == '__main__':
	parser = OptionParser()
	parser.add_option('-c', '--cmd', dest='cmd', help='install or refresh or search or uninstall',metavar='COMMAND')
	parser.add_option('-f', '--field', dest='field', help='field to search on either name or text',metavar='FIELD')
	parser.add_option('-d', '--data', dest='data', help='txt to search',metavar='DATA')
	parser.add_option('-p', '--path', dest='path', help='folder to search in, default is all the folders specified in config',metavar='PATH')
	parser.add_option('-l', '--length', dest='length', help='length of the data including surroundings',metavar='LENGTH')
	parser.add_option('-m', '--mode', dest='mode', help='mode can be fast pr slow, slow means one file at a time, fast means everything together',metavar='MODE')
	(options, args) = parser.parse_args()
	process(options)

	

