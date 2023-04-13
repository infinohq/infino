use log::error;
use tsz::decode::Error;
use tsz::stream::{BufferedReader, BufferedWriter};
use tsz::Encode;
use tsz::{Decode, StdDecoder, StdEncoder};

use crate::ts::data_point::DataPoint;
use crate::utils::error::TsldbError;

/// Decompress the given vector of u8 integers to a DataPoint vector.
pub fn decompress_numeric_vector(compressed: &[u8]) -> Result<Vec<DataPoint>, TsldbError> {
  let r = BufferedReader::new(compressed.to_owned().into_boxed_slice());
  let mut decoder = StdDecoder::new(r);

  let mut done = false;
  let mut tsz_data_points = Vec::new();
  loop {
    if done {
      break;
    }

    match decoder.next() {
      Ok(dp) => {
        tsz_data_points.push(dp);
      }
      Err(err) => {
        if err == Error::EndOfStream {
          done = true;
        } else {
          let err_string = err.to_string();
          error!("Could not decode time series {}", err_string);
          return Err(TsldbError::CannotDecodeTimeSeries(err_string));
        }
      }
    };
  }

  // We need to convert to/from data_point::DataPoint/tsz::DataPoint to avoid tsz::DataPoint in
  // tsldb's public API. In future, to improve performance, we may implement the compression/decompression
  // of data_point::DataPoint directly.
  let data_points = tsz_data_points
    .into_iter()
    .map(DataPoint::new_from_tsz_data_point)
    .collect();

  Ok(data_points)
}

/// Compress a given DataPoint vector to a vector of u8 integers, using delta-of-delta compression.
pub fn compress_data_point_vector(data_points: &[DataPoint]) -> Vec<u8> {
  let start_data_point = data_points.get(0).unwrap();
  let start_time = start_data_point.get_time();

  // We need to convert to/from data_point::DataPoint/tsz::DataPoint to avoid tsz::DataPoint in
  // tsldb's public API. In future, to improve performance, we may implement the compression/decompression
  // of data_point::DataPoint directly.
  let tsz_data_points: Vec<tsz::DataPoint> = data_points
    .iter()
    .map(|dp| dp.get_tsz_data_point())
    .collect();

  let w = BufferedWriter::new();
  let mut encoder = StdEncoder::new(start_time, w);
  for dp in tsz_data_points {
    encoder.encode(dp);
  }

  let bytes = encoder.close();

  bytes.to_vec()
}

#[cfg(test)]
mod tests {
  use super::*;
  use rand::Rng;
  use std::thread;

  #[test]
  fn test_compress_decompress_threads() {
    let num_threads = 20;

    let mut handles = Vec::new();
    for _ in 0..num_threads {
      let handle = thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut expected = Vec::new();
        for _ in 0..128 {
          let time = rng.gen_range(0..10000);
          let dp = DataPoint::new(time, 1.0);
          expected.push(dp);
        }
        expected.sort();

        let compressed = compress_data_point_vector(&expected);
        let received = decompress_numeric_vector(&compressed).unwrap();

        assert_eq!(expected, received);
      });
      handles.push(handle);
    }

    for handle in handles {
      handle.join().unwrap();
    }
  }
}
