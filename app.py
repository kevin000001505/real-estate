import streamlit as st
import pymysql
import pymongo
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
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

def get_data(section: str, region: str, mongo_data: pd.DataFrame, mysql_data: pd.DataFrame) -> pd.DataFrame:
    """Get the filtered data from MongoDB and MySQL"""
    id_list = mongo_data.query("region == @region and section == @section")['id'].tolist()
    data = mysql_data[mysql_data['community_id'].isin(id_list)]
    return data


class Housing_visualization:
    def __init__(self):
        if 'mysql_data' not in st.session_state:
            st.session_state.mysql_data = get_mysql_data()
        if 'mongo_data' not in st.session_state:
            st.session_state.mongo_data = get_mongo_data()

    def pie_chart(self, col: str, section: str, region: str, data: pd.DataFrame, title: str) -> px.pie:
        """For each section, show the distribution of property types as a pie chart"""
        data = data.query("region == @region and section == @section")
        data = data[col].value_counts().reset_index()
        data.columns = ['房屋類型', '數量']
        fig = px.pie(data, values='數量', names='房屋類型', title=title, width=350, height=350)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(legend_title_text='房屋類型')
        return fig

    def line_plot(self, section: str, region: str, title: str, mongo_data: pd.DataFrame, mysql_data: pd.DataFrame) -> px.line:
        """For each section, show the trend of property prices with a year-month slider"""
        data = get_data(section, region, mongo_data, mysql_data)
        data['unit_price'] = pd.to_numeric(data['unit_price'], errors='coerce')
        
        # Group by year-month
        data['year_month'] = data['trans_date'].dt.to_period('M')
        data = data.groupby('year_month')['unit_price'].agg(['mean']).reset_index()
        data['year_month'] = data['year_month'].dt.to_timestamp()

        # Get min and max year-month for the slider
        min_date = data['year_month'].min()
        max_date = data['year_month'].max()

        # Create year-month options for the slider
        date_options = pd.date_range(start=min_date, end=max_date, freq='MS')
        date_options = [d.strftime("%Y-%m") for d in date_options]

        # Create a year-month range slider
        st.write("### Select date range")
        start_date, end_date = st.select_slider(
            "",
            options=date_options,
            value=(min_date.strftime("%Y-%m"), max_date.strftime("%Y-%m"))
        )

        # Convert selected dates back to datetime
        start_date = datetime.strptime(start_date, "%Y-%m")
        end_date = datetime.strptime(end_date, "%Y-%m")

        # Filter data based on selected date range
        mask = (data['year_month'] >= start_date) & (data['year_month'] <= end_date)
        date_filtered_data = data.loc[mask]

        # Create the line plot
        fig = go.Figure()

        fig.add_trace(go.Scatter(x=date_filtered_data['year_month'], y=date_filtered_data['mean'], mode='lines+markers', name='Average Price'))

        fig.update_layout(
            title=title,
            xaxis_title='Year-Month',
            yaxis_title='Average Price',
            hovermode="x unified"
        )
        return fig

    def room(self, section: str, region: str, title: str, mongo_data: pd.DataFrame, mysql_data: pd.DataFrame) -> px.scatter:
        data = get_data(section, region, mongo_data, mysql_data)
        data = data.query("layout_v2 != '0房0廳'")
        room_price = data.groupby('layout_v2').agg({
            'unit_price': 'mean',
            'id': 'count'  # Count of properties for bubble size
        }).reset_index()
        room_price.columns = ['room_number', 'mean_price', 'count']
        room_price['mean_price'] = room_price['mean_price'].astype(float).round(2)
        
        # Extract number of rooms and living rooms
        room_price[['rooms', 'living_rooms']] = room_price['room_number'].str.extract('(\d+)房(\d+)廳')
        room_price['rooms'] = room_price['rooms'].astype(int)
        room_price['living_rooms'] = room_price['living_rooms'].astype(int)
        
        fig = px.scatter(room_price, 
                         x='rooms', 
                         y='mean_price', 
                         size='count',
                         color='living_rooms',
                         hover_name='room_number',
                         labels={'rooms': 'Number of Rooms', 
                                 'mean_price': 'Average Price', 
                                 'living_rooms': 'Number of Living Rooms'},
                         title=title,
                         height=500)
        
        fig.update_traces(marker=dict(sizemin=5))
        fig.update_layout(xaxis=dict(tickmode='linear', dtick=1))
        return fig
    
    def map_plot(self, section: str, region: str, title: str, mongo_data: pd.DataFrame, mysql_data: pd.DataFrame) -> px.choropleth:
        data = get_data(section, region, mongo_data, mysql_data)
        data = data.groupby('community_id')['unit_price'].mean().reset_index()
        data = data.merge(mongo_data[['id', 'name', 'lat', 'lng']], left_on='community_id', right_on='id', how='left')
        data['lat'] = data['lat'].astype(float)
        data['lng'] = data['lng'].astype(float)
        data['unit_price'] = data['unit_price'].astype(float).round(2)

        # Normalize unit_price for size
        data['size'] = (data['unit_price'] - data['unit_price'].min()) / (data['unit_price'].max() - data['unit_price'].min())
        data['size'] = data['size'] * 20 + 5  # Scale size between 5 and 25
        color_scale = [
            [0, "rgb(53,92,125)"],     # Dark Blue
            [0.25, "rgb(61,118,80)"],  # Dark Green
            [0.5, "rgb(142,91,77)"],   # Dark Orange
            [0.75, "rgb(112,68,114)"], # Dark Purple
            [1, "rgb(123,63,67)"]      # Dark Red
        ]
        fig = px.scatter_mapbox(
            data, lat='lat', lon='lng', color ='unit_price', size='size', size_max=25,
            color_continuous_scale=color_scale,
            zoom=12, title=title,
            hover_name='name',  # Show community_id on hover
            hover_data={'unit_price': ':.2f', 'lat': False, 'lng': False, 'size': False},  # Show unit_price on hover, hide others
            text='unit_price'  # Display unit_price as text on the markers
            )
        fig.update_traces(texttemplate='%{text:.0f}', textposition='middle center')
        fig.update_layout(mapbox_style="open-street-map")
        return fig

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
                col1, col2 = st.columns(2)
                with col1:
                    pie_plot = self.pie_chart(col='build_purpose_simple', section=section, region=region, data=mongo_data, title=f'房屋類型分布')
                    st.plotly_chart(pie_plot, use_container_width=True)
                with col2:
                    pass
                romm_plot = self.room(section=section, region=region, title=f'房間數分布', mongo_data=mongo_data, mysql_data=mysql_data)
                st.plotly_chart(romm_plot, use_container_width=True)
                line_plot = self.line_plot(section=section, region=region, title=f'房價趨勢在 {region} {section}', mongo_data=mongo_data, mysql_data=mysql_data)
                st.plotly_chart(line_plot, use_container_width=True)
                map_plot = self.map_plot(section=section, region=region, title=f'房價分布', mongo_data=mongo_data, mysql_data=mysql_data)
                st.plotly_chart(map_plot, use_container_width=True)
if __name__ == "__main__":
    housing_visualization = Housing_visualization()
    housing_visualization.main()