import streamlit as st
import pymysql
import pymongo
import pandas as pd
import plotly.express as px

# Function to get MySQL data
def get_mysql_data():
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='@America155088',
        database='Real_Estate'
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM transform_table")
            result = cursor.fetchall()
        return pd.DataFrame(result)
    finally:
        connection.close()

def get_mongo_data():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client['HousingDataBase']
    collection = db['community_items']
    data = list(collection.find({}))
    return pd.DataFrame(data)

# Streamlit app
def main():
    st.title('台灣房產')

    # Fetch data
    mysql_data = get_mysql_data()
    mongo_data = get_mongo_data()

    regions = mongo_data['region'].unique()
    # Create a dropdown menu in the sidebar for region selection
    region = st.sidebar.selectbox('Select a region:', regions)
    regions_data = mongo_data[mongo_data['region'] == region]
    st.write(f'You selected: {region}')
    if len(regions_data) > 0:
        sections = regions_data[regions_data['region'] == region]['section'].unique()
        section = st.sidebar.selectbox('Select a section:', sections)
        section_data = regions_data[regions_data['section'] == section]
        if section:
            st.write(f'You selected: {section}')
            section_counts = section_data['build_purpose_simple'].value_counts().reset_index()
            section_counts.columns = ['section', 'count']
            fig = px.bar(section_counts, x='section', y='count',
                title=f'Number of Properties in Each Section of {region}')
            st.plotly_chart(fig)


    # Display the selected region
    
if __name__ == "__main__":
    main()