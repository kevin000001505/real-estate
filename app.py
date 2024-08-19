import streamlit as st
import pymysql
import pymongo
import pandas as pd
import plotly.express as px


@st.cache_data
def get_mysql_data(batch_size=1000) -> pd.DataFrame:
    """Get the data from MySQL"""
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='@America155088',
        database='Real_Estate'
    )
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute("SELECT * FROM transform_table")
            result = cursor.fetchall()
        return pd.DataFrame(result)
    finally:
        connection.close()

@st.cache_data
def get_mongo_data() -> pd.DataFrame:
    """Get the data from MongoDB"""
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client['HousingDataBase']
    collection = db['community_items']
    data = list(collection.find({}))
    return pd.DataFrame(data)



class Housing_visualization:
    def __init__(self):
        if 'mysql_data' not in st.session_state:
            st.session_state.mysql_data = get_mysql_data()
        if 'mongo_data' not in st.session_state:
            st.session_state.mongo_data = get_mongo_data()

    def pie_chart(self, col: str, section: str, region: str, data: pd.DataFrame, title: str) -> None:
        """For each section, show the distribution of property types as a pie chart"""
        data = data[(data['region'] == region) & (data['section'] == section)]
        data = data[col].value_counts().reset_index()
        data.columns = ['房屋類型', '數量']
        fig = px.pie(data, values='數量', names='房屋類型', title=title, width=400, height=400)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(legend_title_text='房屋類型')
        st.plotly_chart(fig)

    def line_plot(self, section: str, region: str, title: str, mongo_data: pd.DataFrame, mysql_data: pd.DataFrame) -> None:
        """For each section, show the trend of each type of property"""
        id_list = mongo_data[(mongo_data['region'] == region) & (mongo_data['section'] == section)]['id'].tolist()
        data = mysql_data[mysql_data['community_id'].isin(id_list)]
        data['unit_price'] = pd.to_numeric(data['unit_price'], errors='coerce')
        data = data.groupby(data['trans_date'].dt.to_period('M'))['unit_price'].agg(['mean']).reset_index()
        
        
        fig = px.line(x=data['trans_date'].dt.to_timestamp(), y=data['mean'], title=title, markers=True, line_shape='linear')
        fig.update_traces(
            line=dict(width=3),  # Increase line width
            textposition="top center",  # Position of the text labels
            texttemplate='%{y:.0f}',  # Format of the text labels (no decimal places)
            textfont=dict(size=10)  # Size of the text labels
        )
        fig.update_layout(
            hovermode="x unified",  # Show all Y values for a given X on hover
            yaxis_title="Average Price",
            xaxis_title="Year"
        )
        fig.update_xaxes(title_text='Month', tickfont=dict(size=14))
        fig.update_yaxes(title_text='Average Price', tickfont=dict(size=14))
        st.plotly_chart(fig, use_container_width=True)

    # Streamlit app
    def main(self):
        st.title('台灣房產')

        # Fetch data
        mysql_data = st.session_state.mysql_data
        mongo_data = st.session_state.mongo_data

        regions = mongo_data['region'].unique()
        # Create a dropdown menu in the sidebar for region selection
        region = st.sidebar.selectbox('Select a region:', regions)
        regions_data = mongo_data[mongo_data['region'] == region]
        st.write(f'縣市: {region}')
        if len(regions_data) > 0:
            sections = regions_data[regions_data['region'] == region]['section'].unique()
            section = st.sidebar.selectbox('Select a section:', sections)
            if section:
                st.write(f'地區: {section}')
                self.pie_chart(col='build_purpose_simple', section=section, region=region, data=mongo_data, title=f'房屋類型在 {region} {section}')
                self.line_plot(section=section, region=region, title=f'房屋類型在 {region} {section} 的趨勢', mongo_data=mongo_data, mysql_data=mysql_data)


if __name__ == "__main__":
    housing_visualization = Housing_visualization()
    housing_visualization.main()  