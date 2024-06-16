use anyhow::Result;
use polars::{lazy::dsl::all, prelude::*};
use ron::{extensions::Extensions, ser::PrettyConfig};
use std::{env, fs};

// [table](https://docs.google.com/spreadsheets/d/16PfZTYJBw_qmeDy0Ll1uUUK_kX46SKfdqUw69tSbQ04)
// with_row_index_mut
fn main() -> Result<()> {
    env::set_var("POLARS_FMT_MAX_ROWS", "256");
    env::set_var("POLARS_FMT_MAX_COLS", "256");
    env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256");

    // let fa = df! {
    //     "ID" => &["LnLnLn", "LnLLn", "LaLLa", "LaOC", "MOCy", "MLaLa", "LnLnMo", "LnLnC15:0", "LLLn", "LLLa", "LnOLn", "OOCy", "MLLa", "LaOLa", "LnLnP", "POCy", "PLaLa", "MMLa", "LnLMo", "LLL", "LLPo", "OLLn", "LLM", "OLLa", "OOC", "LnLP", "MLM", "PLLa", "SLnLn", "MOLa", "SOCy", "PMLa", "LLMo", "LLC15:0", "LnLMa", "C20:2LL", "OLL", "OLPo", "OLnO", "LLP", "OLM", "SLLn", "LnOP", "OOLa", "ALnLn", "PLM", "SLLa", "MOM", "POLa", "PLnP", "OLMo", "LLMa", "MoLP", "OLnMa", "GLL", "OLO", "OOPo", "SLL", "OLP", "GOLa", "ALLn", "OOM", "POPo", "SOLn", "BLnLn", "PLP", "SLM", "PPoP", "POM", "SOLa", "SLnP", "OOMo", "OLMa", "C21:0LLn", "MoOP", "C23:0LnLn", "GLO", "OOO", "ALL", "GOM", "BLLn", "SLO", "OOP", "SLP", "BLLa", "SLnS", "AOLa", "POP", "SOM", "PPP", "C23:0LLn", "OOMa", "MaOP", "GOO", "GLS", "BLL", "LgLLn", "ALO", "GOP", "SOO", "ALP", "SLS", "SOP", "AOM", "BOLa", "SPP", "C23:0OLa", "SOMa", "LgLL", "BLO", "GOS", "AOO", "LgLM", "BLP", "ALS", "LgOLa", "AOP", "SOS", "SSP", "C19:0OS", "LgLO", "BOO", "LgLP", "BLS", "AOS", "SSS", "C23:0OO", "LgOO", "LgLS", "LgOP", "BOS", "C25:0OO", "C23:0OS"],
    //     "t_R" => &[48.30, 54.00, 54.80, 55.30, 55.90, 56.60, 57.70, 58.70, 59.60, 60.30, 60.60, 61.40, 61.50, 61.80, 62.10, 63.00, 63.80, 63.80, 63.30, 65.30, 65.70, 66.40, 66.70, 67.00, 67.60, 67.80, 68.20, 68.30, 68.50, 68.50, 69.90, 70.70, 69.00, 70.30, 70.70, 70.80, 71.80, 72.20, 72.60, 73.10, 73.70, 73.80, 74.00, 74.10, 74.30, 75.10, 75.20, 75.50, 75.60, 75.70, 75.60, 76.30, 76.40, 77.00, 77.20, 77.90, 78.30, 79.00, 79.30, 79.40, 79.60, 79.70, 79.80, 80.00, 80.10, 80.90, 80.90, 81.30, 81.30, 81.30, 81.40, 81.50, 82.30, 82.30, 82.70, 82.90, 83.10, 84.00, 84.80, 85.00, 85.10, 85.10, 85.40, 86.60, 86.60, 86.90, 87.00, 87.00, 87.00, 88.70, 87.80, 88.40, 89.70, 89.00, 89.90, 90.00, 90.20, 90.40, 90.40, 90.80, 91.80, 91.90, 92.30, 92.30, 92.30, 94.40, 94.70, 95.00, 94.90, 95.50, 95.70, 96.00, 96.70, 96.80, 96.90, 97.10, 97.50, 97.60, 99.70, 100.20, 100.50, 101.00, 101.90, 102.00, 102.60, 104.60, 103.30, 105.50, 106.50, 106.90, 107.00, 107.70, 109.20],
    // }?;
    let id = Series::new(
        "ID",
        [
            "LnLnLn",
            "LnLLn",
            "LaLLa",
            "LaOC",
            "MOCy",
            "MLaLa",
            "LnLnMo",
            "LnLnC15:0",
            "LLLn",
            "LLLa",
            "LnOLn",
            "OOCy",
            "MLLa",
            "LaOLa",
            "LnLnP",
            "POCy",
            "PLaLa",
            "MMLa",
            "LnLMo",
            "LLL",
            "LLPo",
            "OLLn",
            "LLM",
            "OLLa",
            "OOC",
            "LnLP",
            "MLM",
            "PLLa",
            "SLnLn",
            "MOLa",
            "SOCy",
            "PMLa",
            "LLMo",
            "LLC15:0",
            "LnLMa",
            "C20:2LL",
            "OLL",
            "OLPo",
            "OLnO",
            "LLP",
            "OLM",
            "SLLn",
            "LnOP",
            "OOLa",
            "ALnLn",
            "PLM",
            "SLLa",
            "MOM",
            "POLa",
            "PLnP",
            "OLMo",
            "LLMa",
            "MoLP",
            "OLnMa",
            "GLL",
            "OLO",
            "OOPo",
            "SLL",
            "OLP",
            "GOLa",
            "ALLn",
            "OOM",
            "POPo",
            "SOLn",
            "BLnLn",
            "PLP",
            "SLM",
            "PPoP",
            "POM",
            "SOLa",
            "SLnP",
            "OOMo",
            "OLMa",
            "C21:0LLn",
            "MoOP",
            "C23:0LnLn",
            "GLO",
            "OOO",
            "ALL",
            "GOM",
            "BLLn",
            "SLO",
            "OOP",
            "SLP",
            "BLLa",
            "SLnS",
            "AOLa",
            "POP",
            "SOM",
            "PPP",
            "C23:0LLn",
            "OOMa",
            "MaOP",
            "GOO",
            "GLS",
            "BLL",
            "LgLLn",
            "ALO",
            "GOP",
            "SOO",
            "ALP",
            "SLS",
            "SOP",
            "AOM",
            "BOLa",
            "SPP",
            "C23:0OLa",
            "SOMa",
            "LgLL",
            "BLO",
            "GOS",
            "AOO",
            "LgLM",
            "BLP",
            "ALS",
            "LgOLa",
            "AOP",
            "SOS",
            "SSP",
            "C19:0OS",
            "LgLO",
            "BOO",
            "LgLP",
            "BLS",
            "AOS",
            "SSS",
            "C23:0OO",
            "LgOO",
            "LgLS",
            "LgOP",
            "BOS",
            "C25:0OO",
            "C23:0OS",
        ],
    );
    let t = Series::new(
        "t_R",
        [
            48.30, 54.00, 54.80, 55.30, 55.90, 56.60, 57.70, 58.70, 59.60, 60.30, 60.60, 61.40,
            61.50, 61.80, 62.10, 63.00, 63.80, 63.80, 63.30, 65.30, 65.70, 66.40, 66.70, 67.00,
            67.60, 67.80, 68.20, 68.30, 68.50, 68.50, 69.90, 70.70, 69.00, 70.30, 70.70, 70.80,
            71.80, 72.20, 72.60, 73.10, 73.70, 73.80, 74.00, 74.10, 74.30, 75.10, 75.20, 75.50,
            75.60, 75.70, 75.60, 76.30, 76.40, 77.00, 77.20, 77.90, 78.30, 79.00, 79.30, 79.40,
            79.60, 79.70, 79.80, 80.00, 80.10, 80.90, 80.90, 81.30, 81.30, 81.30, 81.40, 81.50,
            82.30, 82.30, 82.70, 82.90, 83.10, 84.00, 84.80, 85.00, 85.10, 85.10, 85.40, 86.60,
            86.60, 86.90, 87.00, 87.00, 87.00, 88.70, 87.80, 88.40, 89.70, 89.00, 89.90, 90.00,
            90.20, 90.40, 90.40, 90.80, 91.80, 91.90, 92.30, 92.30, 92.30, 94.40, 94.70, 95.00,
            94.90, 95.50, 95.70, 96.00, 96.70, 96.80, 96.90, 97.10, 97.50, 97.60, 99.70, 100.20,
            100.50, 101.00, 101.90, 102.00, 102.60, 104.60, 103.30, 105.50, 106.50, 106.90, 107.00,
            107.70, 109.20,
        ],
    );
    let pecn = {
        let mut ecn: ListPrimitiveChunkedBuilder<Int32Type> =
            ListPrimitiveChunkedBuilder::new("PECN", 8, 8, DataType::Int32);
        ecn.append_slice(&[12, 12, 12]);
        ecn.append_slice(&[12, 14, 12]);
        ecn.append_slice(&[12, 14, 12]);
        ecn.append_slice(&[12, 16, 10]);
        ecn.append_slice(&[14, 16, 8]);
        ecn.append_slice(&[14, 12, 12]);
        ecn.append_slice(&[12, 12, 15]);
        ecn.append_slice(&[12, 12, 15]);
        ecn.append_slice(&[14, 14, 12]);
        ecn.append_slice(&[14, 14, 12]);
        ecn.append_slice(&[12, 16, 12]);
        ecn.append_slice(&[16, 16, 8]);
        ecn.append_slice(&[14, 14, 12]);
        ecn.append_slice(&[12, 16, 12]);
        ecn.append_slice(&[12, 12, 16]);
        ecn.append_slice(&[16, 16, 8]);
        ecn.append_slice(&[16, 12, 12]);
        ecn.append_slice(&[14, 14, 12]);
        ecn.append_slice(&[12, 14, 15]);
        ecn.append_slice(&[14, 14, 14]);
        ecn.append_slice(&[14, 14, 14]);
        ecn.append_slice(&[16, 14, 12]);
        ecn.append_slice(&[14, 14, 14]);
        ecn.append_slice(&[16, 14, 12]);
        ecn.append_slice(&[16, 16, 10]);
        ecn.append_slice(&[12, 14, 16]);
        ecn.append_slice(&[14, 14, 14]);
        ecn.append_slice(&[16, 14, 12]);
        ecn.append_slice(&[18, 12, 12]);
        ecn.append_slice(&[14, 16, 12]);
        ecn.append_slice(&[18, 16, 8]);
        ecn.append_slice(&[16, 14, 12]);
        ecn.append_slice(&[14, 14, 15]);
        ecn.append_slice(&[14, 14, 15]);
        ecn.append_slice(&[12, 14, 17]);
        ecn.append_slice(&[16, 14, 14]);
        ecn.append_slice(&[16, 14, 14]);
        ecn.append_slice(&[16, 14, 14]);
        ecn.append_slice(&[16, 12, 16]);
        ecn.append_slice(&[14, 14, 16]);
        ecn.append_slice(&[16, 14, 14]);
        ecn.append_slice(&[18, 14, 12]);
        ecn.append_slice(&[12, 16, 16]);
        ecn.append_slice(&[16, 16, 12]);
        ecn.append_slice(&[20, 12, 12]);
        ecn.append_slice(&[16, 14, 14]);
        ecn.append_slice(&[18, 14, 12]);
        ecn.append_slice(&[14, 16, 14]);
        ecn.append_slice(&[16, 16, 12]);
        ecn.append_slice(&[16, 12, 16]);
        ecn.append_slice(&[16, 14, 15]);
        ecn.append_slice(&[14, 14, 17]);
        ecn.append_slice(&[15, 14, 16]);
        ecn.append_slice(&[16, 12, 17]);
        ecn.append_slice(&[18, 14, 14]);
        ecn.append_slice(&[16, 14, 16]);
        ecn.append_slice(&[16, 16, 14]);
        ecn.append_slice(&[18, 14, 14]);
        ecn.append_slice(&[16, 14, 16]);
        ecn.append_slice(&[18, 16, 12]);
        ecn.append_slice(&[20, 14, 12]);
        ecn.append_slice(&[16, 16, 14]);
        ecn.append_slice(&[16, 16, 14]);
        ecn.append_slice(&[18, 16, 12]);
        ecn.append_slice(&[22, 12, 12]);
        ecn.append_slice(&[16, 14, 16]);
        ecn.append_slice(&[18, 14, 14]);
        ecn.append_slice(&[16, 14, 16]);
        ecn.append_slice(&[16, 16, 14]);
        ecn.append_slice(&[18, 16, 12]);
        ecn.append_slice(&[18, 12, 16]);
        ecn.append_slice(&[16, 16, 15]);
        ecn.append_slice(&[16, 14, 17]);
        ecn.append_slice(&[21, 14, 12]);
        ecn.append_slice(&[15, 16, 16]);
        ecn.append_slice(&[23, 12, 12]);
        ecn.append_slice(&[18, 14, 16]);
        ecn.append_slice(&[16, 16, 16]);
        ecn.append_slice(&[20, 14, 14]);
        ecn.append_slice(&[18, 16, 14]);
        ecn.append_slice(&[22, 14, 12]);
        ecn.append_slice(&[18, 14, 16]);
        ecn.append_slice(&[16, 16, 16]);
        ecn.append_slice(&[18, 14, 16]);
        ecn.append_slice(&[22, 14, 12]);
        ecn.append_slice(&[18, 12, 18]);
        ecn.append_slice(&[20, 16, 12]);
        ecn.append_slice(&[16, 16, 16]);
        ecn.append_slice(&[18, 16, 14]);
        ecn.append_slice(&[16, 16, 16]);
        ecn.append_slice(&[23, 14, 12]);
        ecn.append_slice(&[16, 16, 17]);
        ecn.append_slice(&[17, 16, 16]);
        ecn.append_slice(&[18, 16, 16]);
        ecn.append_slice(&[18, 14, 18]);
        ecn.append_slice(&[22, 14, 14]);
        ecn.append_slice(&[24, 14, 12]);
        ecn.append_slice(&[20, 14, 16]);
        ecn.append_slice(&[18, 16, 16]);
        ecn.append_slice(&[18, 16, 16]);
        ecn.append_slice(&[20, 14, 16]);
        ecn.append_slice(&[18, 14, 18]);
        ecn.append_slice(&[18, 16, 16]);
        ecn.append_slice(&[20, 16, 14]);
        ecn.append_slice(&[22, 16, 12]);
        ecn.append_slice(&[18, 16, 16]);
        ecn.append_slice(&[23, 16, 12]);
        ecn.append_slice(&[18, 16, 17]);
        ecn.append_slice(&[24, 14, 14]);
        ecn.append_slice(&[22, 14, 16]);
        ecn.append_slice(&[18, 16, 18]);
        ecn.append_slice(&[20, 16, 16]);
        ecn.append_slice(&[24, 14, 14]);
        ecn.append_slice(&[22, 14, 16]);
        ecn.append_slice(&[20, 14, 18]);
        ecn.append_slice(&[24, 16, 12]);
        ecn.append_slice(&[20, 16, 16]);
        ecn.append_slice(&[18, 16, 18]);
        ecn.append_slice(&[18, 18, 16]);
        ecn.append_slice(&[19, 16, 18]);
        ecn.append_slice(&[24, 14, 16]);
        ecn.append_slice(&[22, 16, 16]);
        ecn.append_slice(&[24, 14, 16]);
        ecn.append_slice(&[22, 14, 18]);
        ecn.append_slice(&[20, 16, 18]);
        ecn.append_slice(&[18, 18, 18]);
        ecn.append_slice(&[23, 16, 16]);
        ecn.append_slice(&[24, 16, 16]);
        ecn.append_slice(&[24, 14, 18]);
        ecn.append_slice(&[24, 16, 16]);
        ecn.append_slice(&[22, 16, 18]);
        ecn.append_slice(&[25, 16, 16]);
        ecn.append_slice(&[23, 16, 18]);
        ecn.finish().into_series()
    };
    let peun = {
        let mut u: ListPrimitiveChunkedBuilder<Int32Type> =
            ListPrimitiveChunkedBuilder::new("PESN", 8, 8, DataType::Int32);
        u.append_slice(&[3, 3, 3]);
        u.append_slice(&[3, 2, 3]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[3, 3, 1]);
        u.append_slice(&[3, 3, 0]);
        u.append_slice(&[2, 2, 3]);
        u.append_slice(&[2, 2, 0]);
        u.append_slice(&[3, 1, 3]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[3, 3, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[3, 2, 1]);
        u.append_slice(&[2, 2, 2]);
        u.append_slice(&[2, 2, 1]);
        u.append_slice(&[1, 2, 3]);
        u.append_slice(&[2, 2, 0]);
        u.append_slice(&[1, 2, 0]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[3, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 3, 3]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[2, 2, 1]);
        u.append_slice(&[2, 2, 0]);
        u.append_slice(&[3, 2, 0]);
        u.append_slice(&[2, 2, 2]);
        u.append_slice(&[1, 2, 2]);
        u.append_slice(&[1, 2, 1]);
        u.append_slice(&[1, 3, 1]);
        u.append_slice(&[2, 2, 0]);
        u.append_slice(&[1, 2, 0]);
        u.append_slice(&[0, 2, 3]);
        u.append_slice(&[3, 1, 0]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 3, 3]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 3, 0]);
        u.append_slice(&[1, 2, 1]);
        u.append_slice(&[2, 2, 0]);
        u.append_slice(&[1, 2, 0]);
        u.append_slice(&[1, 3, 0]);
        u.append_slice(&[1, 2, 2]);
        u.append_slice(&[1, 2, 1]);
        u.append_slice(&[1, 1, 1]);
        u.append_slice(&[0, 2, 2]);
        u.append_slice(&[1, 2, 0]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 2, 3]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 1, 1]);
        u.append_slice(&[0, 1, 3]);
        u.append_slice(&[0, 3, 3]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 3, 0]);
        u.append_slice(&[1, 1, 1]);
        u.append_slice(&[1, 2, 0]);
        u.append_slice(&[0, 2, 3]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 3, 3]);
        u.append_slice(&[1, 2, 1]);
        u.append_slice(&[1, 1, 1]);
        u.append_slice(&[0, 2, 2]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 2, 3]);
        u.append_slice(&[0, 2, 1]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 3, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[0, 2, 3]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[1, 1, 1]);
        u.append_slice(&[1, 2, 0]);
        u.append_slice(&[0, 2, 2]);
        u.append_slice(&[0, 2, 3]);
        u.append_slice(&[0, 2, 1]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 1, 1]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 2, 2]);
        u.append_slice(&[0, 2, 1]);
        u.append_slice(&[1, 1, 0]);
        u.append_slice(&[0, 1, 1]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 2, 1]);
        u.append_slice(&[0, 1, 1]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 0, 0]);
        u.append_slice(&[0, 1, 1]);
        u.append_slice(&[0, 1, 1]);
        u.append_slice(&[0, 2, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 0]);
        u.append_slice(&[0, 1, 1]);
        u.append_slice(&[0, 1, 0]);
        u.finish().into_series()
    };
    let df = df! {
        "ID" => &["LnLnLn", "LnLLn", "LaLLa", "LaOC", "MOCy", "MLaLa", "LnLnMo", "LnLnC15:0", "LLLn", "LLLa", "LnOLn", "OOCy", "MLLa", "LaOLa", "LnLnP", "POCy", "PLaLa", "MMLa", "LnLMo", "LLL", "LLPo", "OLLn", "LLM", "OLLa", "OOC", "LnLP", "MLM", "PLLa", "SLnLn", "MOLa", "SOCy", "PMLa", "LLMo", "LLC15:0", "LnLMa", "C20:2LL", "OLL", "OLPo", "OLnO", "LLP", "OLM", "SLLn", "LnOP", "OOLa", "ALnLn", "PLM", "SLLa", "MOM", "POLa", "PLnP", "OLMo", "LLMa", "MoLP", "OLnMa", "GLL", "OLO", "OOPo", "SLL", "OLP", "GOLa", "ALLn", "OOM", "POPo", "SOLn", "BLnLn", "PLP", "SLM", "PPoP", "POM", "SOLa", "SLnP", "OOMo", "OLMa", "C21:0LLn", "MoOP", "C23:0LnLn", "GLO", "OOO", "ALL", "GOM", "BLLn", "SLO", "OOP", "SLP", "BLLa", "SLnS", "AOLa", "POP", "SOM", "PPP", "C23:0LLn", "OOMa", "MaOP", "GOO", "GLS", "BLL", "LgLLn", "ALO", "GOP", "SOO", "ALP", "SLS", "SOP", "AOM", "BOLa", "SPP", "C23:0OLa", "SOMa", "LgLL", "BLO", "GOS", "AOO", "LgLM", "BLP", "ALS", "LgOLa", "AOP", "SOS", "SSP", "C19:0OS", "LgLO", "BOO", "LgLP", "BLS", "AOS", "SSS", "C23:0OO", "LgOO", "LgLS", "LgOP", "BOS", "C25:0OO", "C23:0OS"],
        "t_R" => &[48.30, 54.00, 54.80, 55.30, 55.90, 56.60, 57.70, 58.70, 59.60, 60.30, 60.60, 61.40, 61.50, 61.80, 62.10, 63.00, 63.80, 63.80, 63.30, 65.30, 65.70, 66.40, 66.70, 67.00, 67.60, 67.80, 68.20, 68.30, 68.50, 68.50, 69.90, 70.70, 69.00, 70.30, 70.70, 70.80, 71.80, 72.20, 72.60, 73.10, 73.70, 73.80, 74.00, 74.10, 74.30, 75.10, 75.20, 75.50, 75.60, 75.70, 75.60, 76.30, 76.40, 77.00, 77.20, 77.90, 78.30, 79.00, 79.30, 79.40, 79.60, 79.70, 79.80, 80.00, 80.10, 80.90, 80.90, 81.30, 81.30, 81.30, 81.40, 81.50, 82.30, 82.30, 82.70, 82.90, 83.10, 84.00, 84.80, 85.00, 85.10, 85.10, 85.40, 86.60, 86.60, 86.90, 87.00, 87.00, 87.00, 88.70, 87.80, 88.40, 89.70, 89.00, 89.90, 90.00, 90.20, 90.40, 90.40, 90.80, 91.80, 91.90, 92.30, 92.30, 92.30, 94.40, 94.70, 95.00, 94.90, 95.50, 95.70, 96.00, 96.70, 96.80, 96.90, 97.10, 97.50, 97.60, 99.70, 100.20, 100.50, 101.00, 101.90, 102.00, 102.60, 104.60, 103.30, 105.50, 106.50, 106.90, 107.00, 107.70, 109.20],
        "SN_1.ECN" => [12, 12, 12, 12, 14, 14, 12, 12, 14, 14, 12, 16, 14, 12, 12, 16, 16, 14, 12, 14, 14, 16, 14, 16, 16, 12, 14, 16, 18, 14, 18, 16, 14, 14, 12, 16, 16, 16, 16, 14, 16, 18, 12, 16, 20, 16, 18, 14, 16, 16, 16, 14, 15, 16, 18, 16, 16, 18, 16, 18, 20, 16, 16, 18, 22, 16, 18, 16, 16, 18, 18, 16, 16, 21, 15, 23, 18, 16, 20, 18, 22, 18, 16, 18, 22, 18, 20, 16, 18, 16, 23, 16, 17, 18, 18, 22, 24, 20, 18, 18, 20, 18, 18, 20, 22, 18, 23, 18, 24, 22, 18, 20, 24, 22, 20, 24, 20, 18, 18, 19, 24, 22, 24, 22, 20, 18, 23, 24, 24, 24, 22, 25, 23],
        "SN_2.ECN" => [12, 14, 14, 16, 16, 12, 12, 12, 14, 14, 16, 16, 14, 16, 12, 16, 12, 14, 14, 14, 14, 14, 14, 14, 16, 14, 14, 14, 12, 16, 16, 14, 14, 14, 14, 14, 14, 14, 12, 14, 14, 14, 16, 16, 12, 14, 14, 16, 16, 12, 14, 14, 14, 12, 14, 14, 16, 14, 14, 16, 14, 16, 16, 16, 12, 14, 14, 14, 16, 16, 12, 16, 14, 14, 16, 12, 14, 16, 14, 16, 14, 14, 16, 14, 14, 12, 16, 16, 16, 16, 14, 16, 16, 16, 14, 14, 14, 14, 16, 16, 14, 14, 16, 16, 16, 16, 16, 16, 14, 14, 16, 16, 14, 14, 14, 16, 16, 16, 18, 16, 14, 16, 14, 14, 16, 18, 16, 16, 14, 16, 16, 16, 16],
        "SN_3.ECN" => [12, 12, 12, 10, 08, 12, 15, 15, 12, 12, 12, 08, 12, 12, 16, 08, 12, 12, 15, 14, 14, 12, 14, 12, 10, 16, 14, 12, 12, 12, 08, 12, 15, 15, 17, 14, 14, 14, 16, 16, 14, 12, 16, 12, 12, 14, 12, 14, 12, 16, 15, 17, 16, 17, 14, 16, 14, 14, 16, 12, 12, 14, 14, 12, 12, 16, 14, 16, 14, 12, 16, 15, 17, 12, 16, 12, 16, 16, 14, 14, 12, 16, 16, 16, 12, 18, 12, 16, 14, 16, 12, 17, 16, 16, 18, 14, 12, 16, 16, 16, 16, 18, 16, 14, 12, 16, 12, 17, 14, 16, 18, 16, 14, 16, 18, 12, 16, 18, 16, 18, 16, 16, 16, 18, 18, 18, 16, 16, 18, 16, 18, 16, 18],
        "SN_1.U" => [3, 3, 0, 0, 0, 0, 3, 3, 2, 2, 3, 1, 0, 0, 3, 0, 0, 0, 3, 2, 2, 1, 2, 1, 1, 3, 0, 0, 0, 0, 0, 0, 2, 2, 3, 2, 1, 1, 1, 2, 1, 0, 3, 1, 0, 0, 0, 0, 0, 0, 1, 2, 1, 1, 1, 1, 1, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        "SN_2.U" => [3, 2, 2, 1, 1, 0, 3, 3, 2, 2, 1, 1, 2, 1, 3, 1, 0, 0, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 3, 1, 1, 0, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 1, 1, 3, 2, 2, 1, 1, 3, 2, 2, 2, 3, 2, 2, 1, 2, 2, 1, 2, 1, 1, 1, 3, 2, 2, 1, 1, 1, 3, 1, 2, 2, 1, 3, 2, 1, 2, 1, 2, 2, 1, 2, 2, 3, 1, 1, 1, 0, 2, 1, 1, 1, 2, 2, 2, 2, 1, 1, 2, 2, 1, 1, 1, 0, 1, 1, 2, 2, 1, 1, 2, 2, 2, 1, 1, 1, 0, 1, 2, 1, 2, 2, 1, 0, 1, 1, 2, 1, 1, 1, 1],
        "SN_3.U" => [3, 3, 0, 0, 0, 0, 1, 0, 3, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 3, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 2, 2, 1, 1, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 1, 1, 2, 0, 0, 3, 0, 1, 3, 3, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0, 3, 1, 1, 2, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 1, 0, 2, 3, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0],
        "SN" => peun.clone(),
    }?;
    let df = DataFrame::new(vec![id, t, pecn, peun])?;
    // let t = df.lazy().group_by([col("ID").str().contains("Ln")]);

    let ecn = |i| {
        let position = |j| {
            when(col("PESN").list().get(lit(j), false).eq(lit(i)))
                .then(col("PECN").list().get(lit(j), false))
                .otherwise(lit(0))
        };
        position(0) + position(1) + position(2)
    };
    let esn = |i| {
        let position = |j| {
            when(col("PESN").list().get(lit(j), false).eq(lit(i)))
                .then(lit(1))
                .otherwise(lit(0))
        };
        position(0) + position(1) + position(2)
    };
    let emn = |i| {
        (col("UECN")
            .list()
            .get(lit(i), false)
            .cast(DataType::Float64)
            / col("UESN")
                .list()
                .get(lit(i), false)
                .cast(DataType::Float64))
        .round(2)
        .fill_nan(lit(0))
    };
    let df = df
        .lazy()
        .select([all()])
        .with_columns([
            col("ID"),
            col("t_R"),
            col("PECN"),
            col("PESN"),
            (col("PECN").list().sum()).alias("ECN"),
            (col("PESN").list().sum()).alias("ESN"),
            concat_list([ecn(0), ecn(1), ecn(2), ecn(3)])?.alias("UECN"),
            concat_list([esn(0), esn(1), esn(2), esn(3)])?.alias("UESN"),
            // .map_list(|s| Ok(Some(s * 2)), GetOutput::default())
            // (col("SN_1.U") + col("SN_2.U") + col("SN_3.U")).alias("U"),
        ])
        .with_columns([
            col("UECN")
                .list()
                .eval(
                    (col("").cast(DataType::Float64) / col("*").sum().cast(DataType::Float64))
                        .round(2),
                    false,
                )
                .alias("UERN"),
            concat_list([emn(0), emn(1), emn(2), emn(3)])?.alias("UEMN"),
            // col("UECN")
            //     .list()
            //     .zip_with(col("UEUN").list())?
            //     .alias("UECN.WEIGHTED"),
        ])
        // .with_column(concat_list([emn(0), emn(1), emn(2), emn(3)])?.alias("UEMN"))
        .collect()?;
    println!("df: {df:?}");
    // let series = df.columns(["ID", "SN_1.ECN", "SN_1.U"])?;
    // for series in df.columns(["SN_1.ECN", "SN_1.U"])? {
    //     // series.map();
    //     println!("series: {series:?}");
    // }
    // for index in 0..df.height() {
    //     for series in &series {
    //         let t = &series.get(index)?;
    //         print!("t: {t:?}");
    //     }
    //     println!();
    // }
    let contents = ron::ser::to_string_pretty(
        &df,
        PrettyConfig::default().extensions(Extensions::IMPLICIT_SOME),
    )?;
    fs::write("df.ron", contents)?;
    Ok(())
}

// "SN1.U0"
// "SN1.D1"
// "SN1.D2"
// "SN1.D3"
