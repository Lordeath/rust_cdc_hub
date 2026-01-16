use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomError {
    pub message: String,
    pub error_type: CustomErrorType,
}
#[derive(Debug, Clone, Serialize, Deserialize, Display)]
pub enum CustomErrorType {
    Restart,
}