import pandas as pd
import plotly.graph_objects as go

# 1. Generate the Example Data (Scenario 1: Validity Shift)
data = {
    'day': list(range(1, 21)),
    'validity_rate': [
        98.2, 98.5, 97.9, 98.1, 98.4, 98.0, 98.2, 97.8, 98.3, 98.1, 
        98.5, 97.9, 98.2, 98.0, # Baseline (Days 1-14)
        94.2, 93.8, 94.1, 93.9, 94.0, 93.7  # The Shift (Days 15-20)
    ]
}
df = pd.DataFrame(data)

# 2. Calculate SPC Limits based ONLY on the baseline (first 14 days)
baseline = df.iloc[0:14]['validity_rate']
mu = baseline.mean()
# Moving Range calculation for Sigma estimation
mr = baseline.diff().abs().mean()
sigma_est = mr / 1.128  # 1.128 is the constant for n=2

ucl = mu + (3 * sigma_est)
lcl = mu - (3 * sigma_est)

# 3. Create the Visualization
fig = go.Figure()

# Plot actual data
fig.add_trace(go.Scatter(x=df['day'], y=df['validity_rate'], mode='lines+markers', name='Actual Validity %'))

# Plot Mean and Control Limits
fig.add_hline(y=mu, line_dash="dash", line_color="green", annotation_text="Mean")
fig.add_hline(y=ucl, line_dash="dot", line_color="red", annotation_text="UCL (3-Sigma)")
fig.add_hline(y=lcl, line_dash="dot", line_color="red", annotation_text="LCL (3-Sigma)")

# Highlight the failure zone
fig.add_vrect(x0=14.5, x1=20, fillcolor="red", opacity=0.1, layer="below", line_width=0, annotation_text="Special Cause Detected")

fig.update_layout(title='DQ Monitoring: Validity Rate (X-Chart)', xaxis_title='Day', yaxis_title='% Valid Codes')
fig.show()