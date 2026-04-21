# Fabric notebook source
# %% [markdown]
# # Load Data (Manual)
#
# This notebook is a convenience wrapper. It is called by PostDeploymentConfig
# and can also be run standalone to reload data into the SQL Database.
#
# It reads CSVs from the staging Lakehouse (`CAEManufacturing_LH/Files/data/`)
# and inserts rows into the SQL Database tables.
#
# **Normally you don't need to run this directly.** PostDeploymentConfig does it.

# %%
print("This notebook is called by PostDeploymentConfig.")
print("To reload data manually, copy the 'Step 3' cell from PostDeploymentConfig.")
