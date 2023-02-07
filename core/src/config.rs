use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct KafkaClusterConfig {
    pub bootstrap_servers: String,
    pub security: Option<KafkaClusterSecureConfig>,
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct KafkaClusterSecureConfig {
    pub service_key_location: String,
    pub service_cert_location: String,
    pub ca_pem_location: String,
}
