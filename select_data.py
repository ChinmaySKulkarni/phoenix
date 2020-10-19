#!/usr/bin/python
import os
import sys
import subprocess
sql_file_to_run="selects.sql"

def usage():
	return "python " + sys.argv[0] + " <output file path>"

def create_sql_file(output_fname):
	global sql_file_to_run
	with open(sql_file_to_run, 'w') as fd:
		fd.write("!outputformat csv" +
		 "\n!record " + output_fname + 
		 "\n!tables" +
		 "\nselect * from S.T1;" + 
		 "\nselect * from S.T2;" + 
		 "\nselect * from S1.V1_ON_T1;" + 
		 "\nselect * from S1.V1_ON_T2;" + 
		 "\nselect * from S1.V2_ON_T1;" + 
		 "\nselect * from S1.V2_ON_T2;" + 
		 "\n!record")

def run_sqlline():
	global sql_file_to_run
	try:
		ret_code =  subprocess.call(["./bin/sqlline.py","localhost",sql_file_to_run])
		if ret_code != 0:
			print "Error running sqlline with file: " + sql_file_to_run
	except OSError as err:
		print "OS Error: {0}".format(err)

def normalize_output_file(output_fname):
	print "Normalizing the output file {0}".format(output_fname)
	with open (output_fname, "r") as fd:
		lines = fd.readlines()
	with open (output_fname, "w") as fd:
		for line in lines:
			if "rows selected " not in line and "Saving all output to" not in line:
				fd.write(line)

def remove_sql_file():
	global sql_file_to_run
	print "Removing the temp sql file {0}".format(sql_file_to_run)
	os.remove(sql_file_to_run)

def main():
	if (len(sys.argv) != 2):
		print usage()
	else:
		output_fname = sys.argv[1]
		create_sql_file(output_fname)
		run_sqlline()
		normalize_output_file(output_fname)
		remove_sql_file()


if __name__ == "__main__":
	main()