           
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px

#######################
# Plots
def make_choropleth(input_df, input_id, geojson_data, input_color_theme='blues'):
    choropleth = px.choropleth_mapbox(
            input_df,
            locations="postalcode",
            featureidkey="properties.region",
            geojson=geojson_data,
            color=input_id,
            color_continuous_scale=input_color_theme,
            mapbox_style="carto-positron",
            zoom=7,
            center={"lat": 41.8, "lon": 1.5}
    )
    choropleth.update_traces(
        hovertemplate='<b>Location: %{text} </b><br>' + 'Value: %{customdata:.2f}',
        text = input_df['postalcode'],
        customdata=input_df[input_id] 
    )
    choropleth.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=600
    )
    return choropleth

def energy_character(df,input,postal_codes=['08001'],inputly='month'):
    df = df[df['postalcode'] == postal_codes]
    feature = df[input].values
    consumption = df['consumption'].values
    maxf,minf = max(feature)+0.1*max(feature), min(feature)-0.1*min(feature)
    maxc,minc = max(consumption)+0.1*max(consumption), min(consumption)-0.1*min(consumption)
    fig = go.Figure(
        data=go.Bar(
            x= df[inputly].values,
            y=feature,
            name=input,
            marker=dict(color="lightskyblue"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df[inputly].values,
            y=consumption,
            yaxis="y2",
            name="Consumption",
            marker=dict(color="midnightblue"),
        )
    )

    fig.update_layout(
        legend=dict(orientation="h"),
        yaxis=dict(
            title=dict(text=input),
            side="left",
            range=[minf,maxf],
        ),
        yaxis2=dict(
            title=dict(text="Consumption"),
            side="right",
            range=[minc, maxc],
            overlaying="y",
            tickmode="sync",
        ),
    )
    return fig

def time_series_consumption(df,date,input,postal_codes=['08001'],inputly='hourly'):
    fig, ax = plt.subplots()  # Changed 'x' to 'ax'
    df = df.reset_index()
    # df['time'] = pd.to_datetime(df['time'])  # Ensure 'time' column is in datetime format
    df.set_index(date, inplace=True)  # Set 'time' column as the index
    
    for code in postal_codes:
        # Filter data for the postal code
        postal_code_data = df[df['postalcode'] == code]
        hourly_data = postal_code_data[input]
        
        # Plot the data
        ax.plot(hourly_data.index, hourly_data, marker='o', linestyle='-', label=f'Postal Code: {code}')
    
    # Adding labels and title
    ax.set_title(f'{inputly} {input} for Selected Region')  # Corrected to ax.set_title
    ax.set_xlabel(date)  # Corrected to ax.set_xlabel
    ax.set_ylabel(input)  # Corrected to ax.set_ylabel
    ax.legend()  # Display legend
    ax.grid()  # Add grid lines
    plt.xticks(rotation=45)  # Rotate x-axis labels
    plt.tight_layout()  # Adjust layout to prevent overlapping
    
    return fig