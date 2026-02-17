variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "rg-gridwatch-nuclear-dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "centralus"  # "eastus" and eastus2" hit capacity constraints
}

variable "storage_account_name" {
  description = "Name of the Data Lake Storage"
  type        = string
  # Changed to be under 24 chars.
  default     = "gridwatchcn11302025"
}

variable "eia_api_key" {
  description = "API Key for the US Energy Information Administration"
  type        = string
  sensitive   = true # This hides the key from CLI logs for security
}
