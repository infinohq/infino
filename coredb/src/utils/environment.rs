use dotenv::dotenv;

pub fn load_env() {
  // Load environment variables from .env file, if it exists.
  dotenv().ok();

  // Load environment variables from .env-creds file, if it exists.
  dotenv::from_filename(".env-creds").ok();
}
