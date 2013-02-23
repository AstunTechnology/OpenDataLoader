from __future__ import with_statement
import os,os.path,sys,glob
import shlex, subprocess
import string, csv
import psycopg2


config_file = sys.argv[1]
requested_dataset = sys.argv[2]

class OSOpenLoader:

    def __init__(self):
        pass

    def run(self, config):
        self.read_config(config)
        self.load()


    def read_config(self, config):
        self.config = config
        self.src_dir = config['src_dir']
        self.csv_table = config['csv_table']
        self.database = config['database']
        if not os.path.exists(self.src_dir):
                print 'Filepath does not exist'

    def load (self):

        try:
            conn=psycopg2.connect(self.database)

        except:
            print "Unable to connect to the database"

        # Open table with datasets available and parameters used by this script
        table = open(self.csv_table)
        # Read the header line and get the important field indices
        headerLine = table.readline()
        headerLine = headerLine.rstrip("\n")
        segmentedHeaderLine = headerLine.split(";")

        # Figure out positions of all parameters in the table
        datasetIndex = segmentedHeaderLine.index("Dataset")
        location_folderIndex = segmentedHeaderLine.index("Location_folder")
        schemaIndex = segmentedHeaderLine.index("Schema")
        source_ProjectionIndex = segmentedHeaderLine.index("Source_Projection")
        vrt_filenameIndex = segmentedHeaderLine.index("VRT_filename")
        csv_filenameIndex = segmentedHeaderLine.index("CSV_filename")
        headerIndex = segmentedHeaderLine.index("Header")
        delimiterIndex = segmentedHeaderLine.index("Delimiter")

        # Loop through each line in the table, splitting it with delimiter ";"
        for line in table.readlines():
            line = line.rstrip("\n")
            segmentedLine = line.split(";")

            # Create variables for all needed field items based on index position
            dataset = segmentedLine[datasetIndex]
            location_folder = segmentedLine[location_folderIndex]
            schema = segmentedLine[schemaIndex]
            source_projection = segmentedLine[source_ProjectionIndex]
            vrt_filename = segmentedLine[vrt_filenameIndex]
            csv_filename = segmentedLine[csv_filenameIndex]
            header = segmentedLine[headerIndex]
            delimiter = segmentedLine[delimiterIndex]

            # Find requested dataset name in the Table.csv and load files using parameters from it
            if dataset==requested_dataset:
                for folder in glob.glob(os.path.join(self.src_dir, location_folder)):
                    folder_path=os.path.join(self.src_dir, location_folder)
                    if os.path.exists(folder_path):
                        num_files = 0
                        seen_files = []
                        for root, dirs, files in os.walk(folder_path):
                            for name in files:
                                if name.endswith(".shp"):
                                    file_path=os.path.join(folder_path,root,name)
                                    print "Processing: %s" % file_path
                                    if name in seen_files:
                                        appover = "append"
                                    else:
                                        appover = "overwrite"
                                        seen_files.append(name)

                                    if "text" in name or "Text" in name:
                                        os.environ["PGCLIENTENCODING"] = "LATIN1"
                                    else:
                                        os.environ["PGCLIENTENCODING"] = "UTF8"

                                    ogr_cmd="ogr2ogr -%s -skipfailures -f PostgreSQL PG:'%s active_schema=%s' -s_srs %s -a_srs EPSG:27700 -lco PRECISION=NO -nlt GEOMETRY" %(appover,self.database,schema,source_projection)
                                    args = shlex.split(ogr_cmd)
                                    args.append(file_path)
                                    rtn = subprocess.call(args)
                                    num_files += 1

                                if name==csv_filename:
                                    original_csv=os.path.join(folder_path,root,name)
                                    new_csv=os.path.join(folder_path,root,name[:-4]+"_new.csv")
                                    if os.path.exists(original_csv):
                                        print "Editing %s and creating %s" % (original_csv,new_csv)
                                        header_string= header.split()
                                        with open(original_csv, 'r') as original_csv:
                                            with open(new_csv, 'w') as new_csv:
                                                row = header_string
                                                header = csv.writer(new_csv,delimiter=';')
                                                header.writerow(row)
                                                for row in original_csv:
                                                    new_delimiter = row.replace(delimiter,";")
                                                    new_csv.write(new_delimiter)

                                    else:
                                        print "No correct CSV file found"
                                        sys.exit()
                                    vrtfile_path=os.path.join(folder_path,root,vrt_filename)
                                    if os.path.exists(vrtfile_path):
                                        print "Loading csv data using %s" % vrtfile_path
                                        if requested_dataset=="Gazetteer" or "GAZETTEER" in name:
                                            os.environ["PGCLIENTENCODING"] = "LATIN1"
                                        ogr_cmd="ogr2ogr -overwrite  -skipfailures -f PostgreSQL PG:'%s active_schema=%s' -s_srs %s -a_srs EPSG:27700" %(self.database,schema,source_projection)
                                        args = shlex.split(ogr_cmd)
                                        args.append(vrtfile_path)
                                        rtn = subprocess.call(args)
                                    else:
                                        print "No correct VRT file found. Cannot load %s data" % dataset
                                        sys.exit()
                                    print "Successfully loaded CSV data from %s" % dataset

                        print "Loaded %s shapefiles" % num_files

                    if not os.path.exists(folder_path):
                        print "Path to folder is not correct"
                        sys.exit()


def main():
    if os.path.exists(config_file):
        # Build a dict of configuration
        with open(config_file, 'r') as f:
            config = dict([line.replace('\n','').split('=',1) for line in f.readlines() if len(line.replace('\n','')) and line[0:1] != '#'])
        loader = OSOpenLoader()
        loader.run(config)
    else:
        print 'Could not find config file:', config_file



if __name__ == '__main__':
    main()





