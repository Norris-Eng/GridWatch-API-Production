# 1. Resource Group
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# 2. Azure Data Lake Storage Gen2
# Standard Blob Storage is flat. ADLS Gen2 allows folders (Hierarchical Namespace).
# For Big Data analytics.
resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS" # Locally Redundant Storage (Cheapest)
  account_kind             = "StorageV2"

  is_hns_enabled           = true

  custom_domain {
    name          = "gridwatch.live"
    use_subdomain = true
  }
}

# 3. Create 'landing' container inside the Data Lake
resource "azurerm_storage_data_lake_gen2_filesystem" "raw_data" {
  name               = "raw-bronze"
  storage_account_id = azurerm_storage_account.adls.id
}

# 4. App Service Plan (Consumption / Serverless)
# The 'Y1' SKU is the dynamic consumption plan.
# Only paying when the code runs. Fitting for intermittent scrapers.
resource "azurerm_service_plan" "asp" {
  name                = "asp-gridwatch-nuclear"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  os_type             = "Linux" # Python functions run best on Linux
  sku_name            = "Y1"
}

resource "azurerm_log_analytics_workspace" "law" {
  name                = "law-gridwatch-${var.location}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
}

# Application Insights (Monitoring)
resource "azurerm_application_insights" "app_insights" {
  name                = "ai-gridwatch-${var.location}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  workspace_id        = azurerm_log_analytics_workspace.law.id
  application_type    = "web"
}

# Output the Storage Account Name as evidence it worked
output "adls_name" {
  value = azurerm_storage_account.adls.name
}

# 5. The Function App (The Logic Container)
# This is the actual resource that will host the Python code.
# It sits on top of the App Service Plan.
resource "azurerm_linux_function_app" "function_app" {
  name                = "fa-gridwatch-nuclear-${var.location}" # naming convention based on region
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  storage_account_name       = azurerm_storage_account.adls.name
  storage_account_access_key = azurerm_storage_account.adls.primary_access_key
  service_plan_id            = azurerm_service_plan.asp.id

  site_config {
    application_stack {
      python_version = "3.11" # Using Python 3.11 for stability
    }
  }

  # Environment variables for the function
  app_settings = {
    "BUILD_FLAGS"                    = "UseExpressBuild"
    "ENABLE_ORYX_BUILD"              = "true"
    "SCM_DO_BUILD_DURING_DEPLOYMENT" = "true"
    "X_FUNCTIONS_WORKER_RUNTIME"     = "python"
    "AzureWebJobsStorage"            = azurerm_storage_account.adls.primary_connection_string
    "APPLICATIONINSIGHTS_CONNECTION_STRING" = azurerm_application_insights.app_insights.connection_string

    "EIA_API_KEY"                    = var.eia_api_key
  }

  # IGNORE changes to these settings so Terraform doesn't wipe them next time
  lifecycle {
    ignore_changes = [
      app_settings["WEBSITE_RUN_FROM_PACKAGE"],
      app_settings["GRIDWATCH_INTERNAL_KEY"],
      tags
    ]
  }
}

# Output the Function App Name
output "function_app_name" {
  value = azurerm_linux_function_app.function_app.name
}

# 6. Silver Container (Processed Data)
# Medallion Architecture. Bronze = Raw. Silver = Clean/Enriched.
resource "azurerm_storage_data_lake_gen2_filesystem" "processed_data" {
  name               = "processed-silver"
  storage_account_id = azurerm_storage_account.adls.id
}

# 7. Hot Storage (Table for Real-Time API)
# This replaces file scanning for the /status endpoint.
resource "azurerm_storage_table" "gridstatus_table" {
  name                 = "GridStatusLatest"
  storage_account_name = azurerm_storage_account.adls.name
}

resource "azurerm_storage_table" "gridstatus_history" {
  name                 = "GridStatusHistory"
  storage_account_name = azurerm_storage_account.adls.name
}
