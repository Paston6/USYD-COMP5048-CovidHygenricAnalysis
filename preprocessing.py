# =================
# 5048 Assignment2 
#==================
import pandas as pd
import datetime


# global
path = "./DATA/"
file = "covid4.csv"

#====================
#    Read CSV file
#====================
# path -> string
def read_file(path):
	# log read_csv()
	print("...read_csv(): Reading file at "+path)

	try:
		raw_data = pd.read_csv(path)
	except:
		# log read_csv() error
		print("...read_csv(): Can not read "+path)
		return None

	# log read_csv() sucess
	print("...read_csv(): Success "+path +"\n")
	return raw_data


#======================
#    Describe Data
#======================
# data -> pd/dataframe
def describe_data(data, describe = False, head = False, head_num = 5):
	# log describe_data()
	print("...describe_data():")
	print("......" + str(list(data)))
	if (describe != False):
		print(data.describe())
	if (head != False):
		print(data.head(head_num))

	print(+"\n")

#======================
#  aggregate by month
#======================
def aggregation(data, group_by = False, method = "sum"):
	#data["cases"] = data.groupby([group_by]).transform("count")
	data["cases"] = ""
	if (method == "count"):	
		new_data = data.groupby([group_by])["cases"].count().reset_index(name="cases")

	return new_data


#======================
#     Drop columns
#======================
# data -> pd/dataframe
# name -> list[string]
def drop_column(data, names):
	data = data.drop(names, axis=1)
	return data

def save_df_as_csv(data, path):
	data.to_csv(path)

def rbind(data, path, col):
	join = read_file(path)
	join = drop_column(join, ["Total","VIC","QLD","WA","SA","TAS","ACT","NT"])

	join = join.rename(columns={'Date': 'notification_date', 'NSW': 'death'})
	join["notification_date"] = pd.to_datetime(join["notification_date"])
	result = pd.merge(data, join, on=['notification_date'])
	return result

#===============
# Main Function
#===============

if __name__ == "__main__":
	# Read csv file as df
    raw_data = read_file(path+file)
    raw_data = drop_column(raw_data, ['lhd_2010_code', 'lhd_2010_name', 'lga_code19', 'lga_name19'])
    new_data = aggregation(raw_data, group_by = "notification_date", method = "count")
    #print(type(new_data["notification_date"]))

    new_data["notification_date"] = pd.to_datetime(new_data["notification_date"])

    p1 = "/Users/xingxing/Desktop/5048Assignment/AU_COVID19-master/time_series_deaths.csv"
    new_data = rbind(new_data, p1, "notification_date")
    dg = new_data.groupby(pd.Grouper(key='notification_date', freq='1M')).sum()
    dg.index = dg.index.strftime('%Y-%B')

    new_data['death'] = new_data['death'].shift(-1) - new_data['death']
    new_data.drop([len(new_data)-1],inplace=True)
    new_data[['death']] = new_data[['death']].apply(pd.to_numeric)
    new_data.death = new_data.death.astype(int)
    
    applied = []
    for i in  new_data['notification_date']:
        applied.append(i < datetime.date(2020, 3, 4))
    
    new_data['applied'] = applied
    print(new_data)
    save_df_as_csv(new_data, path+"case_death.csv")
    save_df_as_csv(dg, path+"case_death_month.csv")




