use anyhow::Result;
use polars::{
    lazy::dsl::{all, concat_str},
    prelude::*,
};
use ron::{extensions::Extensions, ser::PrettyConfig};
use std::{env, fs};

// [table](https://docs.google.com/spreadsheets/d/16PfZTYJBw_qmeDy0Ll1uUUK_kX46SKfdqUw69tSbQ04)
// with_row_index_mut
fn main() -> Result<()> {
    env::set_var("POLARS_FMT_MAX_COLS", "256");
    env::set_var("POLARS_FMT_MAX_ROWS", "256");
    env::set_var("POLARS_FMT_TABLE_CELL_LIST_LEN", "256");
    env::set_var("POLARS_FMT_STR_LEN", "256");

    // (Cy) -> [$1]
    let fa = df! {
        "CN" => [8, 10, 12, 14, 15, 16, 16, 17, 17, 18, 18, 18, 18, 19, 20, 20, 20, 21, 22, 23, 24, 25],
        "DB" => [0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 2, 3, 0, 0, 1, 2, 0, 0, 0, 0, 0],
        "Delta" => [None, None, None, None, None, None, Some(9), None, Some(10), None, Some(9), Some(9), Some(9), None, None, Some(11), Some(11), None, None, None, None, None],
        "Omega" => [None, None, None, None, None, None, Some(9), None, Some(10), None, Some(9), Some(12), Some(15), None, None, Some(11), Some(14), None, None, None, None, None],
        "Abbreviation" => ["Cy", "C", "La", "M", "–", "P", "Po", "Ma", "Mo", "S", "O", "L", "Ln", "–", "A", "G", "–", "–", "B", "–", "Lg", "–"],
        "Trivial name" => ["Caprylic", "Capric", "Lauric", "Myristic", "–", "Palmitic", "Palmitoleic", "Margaric", "Margaroleic", "Stearic", "Oleic", "Linoleic", "Linolenic", "–", "Arachidic", "Gadoleic", "–", "–", "Behenic", "–", "Lignoceric", "–"],
        "Systematic name" => ["Octanoic", "Decanoic", "Dodecanoic", "Tetradecanoic", "Pentadecanoic", "Hexadecanoic", "cis-9-Hexadecenoic", "Heptadecanoic", "cis-10-Heptadecenoic", "Octadecanoic", "cis-9-Octadecenoic", "cis-9,12-Octadecadienoic", "cis-9,12,15-Octadecatrienoic", "Nonadecanoic", "Eicosanoic", "cis-11-Eicosenoic", "cis-11,14-Eicosadienoic", "Heneicosanoic", "Docosanoic", "Tricosanoic", "Tetracosanoic", "Pentacosanoic"],
    }?;
    let fa = fa
        .lazy()
        .with_column((col("CN") - lit(2) * col("DB")).alias("ECN"))
        .collect()?;
    println!("fa: {fa}");

    let cn_sn1 = Series::new(
        "CN.SN_1",
        [
            18, 18, 12, 12, 14, 14, 18, 18, 18, 18, 18, 18, 14, 12, 18, 16, 16, 14, 18, 18, 18, 18,
            18, 18, 18, 18, 14, 16, 18, 14, 18, 16, 18, 18, 18, 20, 18, 18, 18, 18, 18, 18, 18, 18,
            20, 16, 18, 14, 16, 16, 18, 18, 17, 18, 20, 18, 18, 18, 18, 20, 20, 18, 16, 18, 22, 16,
            18, 16, 16, 18, 18, 18, 18, 21, 17, 23, 20, 18, 20, 20, 22, 18, 18, 18, 22, 18, 20, 16,
            18, 16, 23, 18, 17, 20, 20, 22, 24, 20, 20, 18, 20, 18, 18, 20, 22, 18, 23, 18, 24, 22,
            20, 20, 24, 22, 20, 24, 20, 18, 18, 19, 24, 22, 24, 22, 20, 18, 23, 24, 24, 24, 22, 25,
            23,
        ],
    );
    let cn_sn2 = Series::new(
        "CN.SN_2",
        [
            18, 18, 18, 18, 18, 12, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 12, 14, 18, 18, 18, 18,
            18, 18, 18, 18, 18, 18, 18, 18, 18, 14, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18,
            18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18,
            18, 16, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18,
            18, 16, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 16, 18, 18, 18, 18,
            18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18, 18,
            18,
        ],
    );
    let cn_sn3 = Series::new(
        "CN.SN_3",
        [
            18, 18, 12, 10, 08, 12, 17, 15, 18, 12, 18, 08, 12, 12, 16, 08, 12, 12, 17, 18, 16, 18,
            14, 12, 10, 16, 14, 12, 18, 12, 08, 12, 17, 15, 17, 18, 18, 16, 18, 16, 14, 18, 16, 12,
            18, 14, 12, 14, 12, 16, 17, 17, 16, 17, 18, 18, 16, 18, 16, 12, 18, 14, 16, 18, 18, 16,
            14, 16, 14, 12, 16, 17, 17, 18, 16, 18, 18, 18, 18, 14, 18, 18, 16, 16, 12, 18, 12, 16,
            14, 16, 18, 17, 16, 18, 18, 18, 18, 18, 16, 18, 16, 18, 16, 14, 12, 16, 12, 17, 18, 18,
            18, 18, 14, 16, 18, 12, 16, 18, 16, 18, 18, 18, 16, 18, 18, 18, 18, 18, 18, 16, 18, 18,
            18,
        ],
    );
    let db_sn1 = Series::new(
        "DB.SN_1",
        [
            3, 3, 0, 0, 0, 0, 3, 3, 2, 2, 3, 1, 0, 0, 3, 0, 0, 0, 3, 2, 2, 1, 2, 1, 1, 3, 0, 0, 0,
            0, 0, 0, 2, 2, 3, 2, 1, 1, 1, 2, 1, 0, 3, 1, 0, 0, 0, 0, 0, 0, 1, 2, 1, 1, 1, 1, 1, 0,
            1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 1, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
    );
    let db_sn2 = Series::new(
        "DB.SN_2",
        [
            3, 2, 2, 1, 1, 0, 3, 3, 2, 2, 1, 1, 2, 1, 3, 1, 0, 0, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 3,
            1, 1, 0, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 1, 1, 3, 2, 2, 1, 1, 3, 2, 2, 2, 3, 2, 2, 1, 2,
            2, 1, 2, 1, 1, 1, 3, 2, 2, 1, 1, 1, 3, 1, 2, 2, 1, 3, 2, 1, 2, 1, 2, 2, 1, 2, 2, 3, 1,
            1, 1, 0, 2, 1, 1, 1, 2, 2, 2, 2, 1, 1, 2, 2, 1, 1, 1, 0, 1, 1, 2, 2, 1, 1, 2, 2, 2, 1,
            1, 1, 0, 1, 2, 1, 2, 2, 1, 0, 1, 1, 2, 1, 1, 1, 1,
        ],
    );
    let db_sn3 = Series::new(
        "DB.SN_3",
        [
            3, 3, 0, 0, 0, 0, 1, 0, 3, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 2, 1, 3, 0, 0, 0, 0, 0, 0, 3,
            0, 0, 0, 1, 0, 0, 2, 2, 1, 1, 0, 0, 3, 0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 1, 1, 2,
            0, 0, 3, 0, 1, 3, 3, 0, 0, 0, 0, 0, 0, 1, 0, 3, 0, 3, 1, 1, 2, 0, 3, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 3, 0, 0, 1, 0, 2, 3, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0,
        ],
    );
    let t_r = Series::new(
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
    let df = DataFrame::new(vec![cn_sn1, cn_sn2, cn_sn3, db_sn1, db_sn2, db_sn3, t_r])?;
    // let df = df
    //     .lazy()
    //     .group_by([
    //         "CN.SN_1", "CN.SN_2", "CN.SN_3", "DB.SN_1", "DB.SN_2", "DB.SN_3",
    //     ])
    //     .agg([
    //         concat_list([
    //             concat_str([col("CN.SN_1"), col("DB.SN_1")], ":", false),
    //             concat_str([col("CN.SN_2"), col("DB.SN_2")], ":", false),
    //             concat_str([col("CN.SN_3"), col("DB.SN_3")], ":", false),
    //         ])?
    //         .alias("DB.SN"),
    //         len(),
    //     ])
    //     .collect()?;
    let df = df
        .lazy()
        .join(
            fa.clone().lazy().select([
                col("CN"),
                col("DB"),
                col("Delta").alias("Delta.SN_1"),
                col("Omega").alias("Omega.SN_1"),
                col("Abbreviation").alias("Abbreviation.SN_1"),
                col("ECN").alias("ECN.SN_1"),
            ]),
            [col("CN.SN_1"), col("DB.SN_1")],
            [col("CN"), col("DB")],
            JoinArgs::new(JoinType::Left),
        )
        .join(
            fa.clone().lazy().select([
                col("CN"),
                col("DB"),
                col("Delta").alias("Delta.SN_2"),
                col("Omega").alias("Omega.SN_2"),
                col("Abbreviation").alias("Abbreviation.SN_2"),
                col("ECN").alias("ECN.SN_2"),
            ]),
            [col("CN.SN_2"), col("DB.SN_2")],
            [col("CN"), col("DB")],
            JoinArgs::new(JoinType::Left),
        )
        .join(
            fa.clone().lazy().select([
                col("CN"),
                col("DB"),
                col("Delta").alias("Delta.SN_3"),
                col("Omega").alias("Omega.SN_3"),
                col("Abbreviation").alias("Abbreviation.SN_3"),
                col("ECN").alias("ECN.SN_3"),
            ]),
            [col("CN.SN_3"), col("DB.SN_3")],
            [col("CN"), col("DB")],
            JoinArgs::new(JoinType::Left),
        )
        .select([
            all(),
            concat_list([
                col("Abbreviation.SN_1"),
                col("Abbreviation.SN_2"),
                col("Abbreviation.SN_3"),
            ])?
            .alias("Abbreviation.SN"),
            concat_list([
                concat_str([col("CN.SN_1"), col("DB.SN_1")], ":", false),
                concat_str([col("CN.SN_2"), col("DB.SN_2")], ":", false),
                concat_str([col("CN.SN_3"), col("DB.SN_3")], ":", false),
            ])?
            .alias("CN:DB.SN"),
            (col("ECN.SN_1") + col("ECN.SN_2") + col("ECN.SN_3")).alias("ECN"),
            cnt_db(0).alias("CNT.DB_0"),
            cnt_db(1).alias("CNT.DB_1"),
            cnt_db(2).alias("CNT.DB_2"),
            cnt_db(3).alias("CNT.DB_3"),
            ecn_db(0).alias("ECN.DB_0"),
            ecn_db(1).alias("ECN.DB_1"),
            ecn_db(2).alias("ECN.DB_2"),
            ecn_db(3).alias("ECN.DB_3"),
            delta_db(0).alias("Delta.DB_0"),
            delta_db(1).alias("Delta.DB_1"),
            delta_db(2).alias("Delta.DB_2"),
            delta_db(3).alias("Delta.DB_3"),
            omega_db(0).alias("Omega.DB_0"),
            omega_db(1).alias("Omega.DB_1"),
            omega_db(2).alias("Omega.DB_2"),
            omega_db(3).alias("Omega.DB_3"),
        ])
        .with_columns([
            // Relative ECN
            (col("ECN.DB_0").cast(DataType::Float64) / col("ECN"))
                .round(2)
                .alias("RECN.DB_0"),
            (col("ECN.DB_1").cast(DataType::Float64) / col("ECN"))
                .round(2)
                .alias("RECN.DB_1"),
            (col("ECN.DB_2").cast(DataType::Float64) / col("ECN"))
                .round(2)
                .alias("RECN.DB_2"),
            (col("ECN.DB_3").cast(DataType::Float64) / col("ECN"))
                .round(2)
                .alias("RECN.DB_3"),
            // Median ECN
            (col("ECN.DB_0").cast(DataType::Float64) / col("CNT.DB_0"))
                .fill_nan(lit(0))
                .round(2)
                .alias("MECN.DB_0"),
            (col("ECN.DB_1").cast(DataType::Float64) / col("CNT.DB_1"))
                .fill_nan(lit(0))
                .round(2)
                .alias("MECN.DB_1"),
            (col("ECN.DB_2").cast(DataType::Float64) / col("CNT.DB_2"))
                .fill_nan(lit(0))
                .round(2)
                .alias("MECN.DB_2"),
            (col("ECN.DB_3").cast(DataType::Float64) / col("CNT.DB_3"))
                .fill_nan(lit(0))
                .round(2)
                .alias("MECN.DB_3"),
        ])
        // .with_columns([
        //     // (col("ECN.SN_1").cast(DataType::Float64) / col("ECN"))
        //     //     .round(2)
        //     //     .alias("RECN.SN_1"),
        //     // (col("ECN.SN_2").cast(DataType::Float64) / col("ECN"))
        //     //     .round(2)
        //     //     .alias("RECN.SN_2"),
        //     // (col("ECN.SN_3").cast(DataType::Float64) / col("ECN"))
        //     //     .round(2)
        //     //     .alias("RECN.SN_3"),
        // ])
        .sort(["t_R"], Default::default())
        .select([
            // as_struct(vec![
            //     col("CNT.DB_0")
            //         .gt_eq(col("CNT.DB_0").shift(lit(1)))
            //         .fill_null(lit(true))
            //         .alias("S"),
            //     col("ECN")
            //         .gt(col("ECN").shift(lit(1)))
            //         .or(col("CNT.DB_0").gt_eq(col("CNT.DB_0").shift(lit(1))))
            //         .fill_null(lit(true))
            //         .alias("FULL"),
            // ])
            // .alias("OK"),
            // col("Abbreviation.SN"),
            col("ECN")
                .gt_eq(col("ECN").shift(lit(1)))
                .fill_null(lit(true))
                .alias("OK.ECN"),
            col("ECN")
                .gt(col("ECN").shift(lit(1)))
                .or(col("CNT.DB_0").gt_eq(col("CNT.DB_0").shift(lit(1))))
                .fill_null(lit(true))
                .alias("OK.DB_0"),
            col("ECN")
                .gt(col("ECN").shift(lit(1)))
                .or(col("CNT.DB_0")
                    .gt(col("CNT.DB_0").shift(lit(1)))
                    .or(col("CNT.DB_0")
                        .eq(col("CNT.DB_0").shift(lit(1)))
                        .and(col("CNT.DB_1").gt_eq(col("CNT.DB_1").shift(lit(1))))))
                .fill_null(lit(true))
                .alias("OK.DB_1"),
            col("CN:DB.SN"),
            col("ECN"),
            col("t_R"),
            // concat_list([col(r#"^Delta\.DB_\d+$"#)])?.alias("Delta.DB"),
            // concat_list([col(r#"^Omega\.DB_\d+$"#)])?.alias("Omega.DB"),
            concat_list([col(r#"^CNT\.DB_\d+$"#)])?.alias("CNT.DB"),
            concat_list([col(r#"^ECN\.DB_\d+$"#)])?.alias("ECN.DB"),
            concat_list([col(r#"^RECN\.DB_\d+$"#)])?.alias("RECN.DB"),
            concat_list([col(r#"^MECN\.DB_\d+$"#)])?.alias("MECN.DB"),
        ])
        .collect()?;
    println!("df: {df}");
    // let df = df
    //     .lazy()
    //     .group_by([col("ECN"), col("CNT.DB_0")])
    //     .agg([col("t_R"), col("CNT.DB")])
    //     .sort(["ECN", "CNT.DB_0"], Default::default())
    //     .collect()?;
    // println!("df: {df}");
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

fn delta_db(db: i32) -> Expr {
    (when(col("DB.SN_1").eq(lit(db)))
        .then(col("Delta.SN_1"))
        .otherwise(lit(0))
        + when(col("DB.SN_2").eq(lit(db)))
            .then(col("Delta.SN_2"))
            .otherwise(lit(0))
        + when(col("DB.SN_3").eq(lit(db)))
            .then(col("Delta.SN_3"))
            .otherwise(lit(0)))
    .fill_null(lit(0))
}

fn omega_db(db: i32) -> Expr {
    (when(col("DB.SN_1").eq(lit(db)))
        .then(col("Omega.SN_1"))
        .otherwise(lit(0))
        + when(col("DB.SN_2").eq(lit(db)))
            .then(col("Omega.SN_2"))
            .otherwise(lit(0))
        + when(col("DB.SN_3").eq(lit(db)))
            .then(col("Omega.SN_3"))
            .otherwise(lit(0)))
    .fill_null(lit(0))
}

fn cnt_db(db: i32) -> Expr {
    when(col("DB.SN_1").eq(lit(db)))
        .then(lit(1))
        .otherwise(lit(0))
        + when(col("DB.SN_2").eq(lit(db)))
            .then(lit(1))
            .otherwise(lit(0))
        + when(col("DB.SN_3").eq(lit(db)))
            .then(lit(1))
            .otherwise(lit(0))
}

fn ecn_db(db: i32) -> Expr {
    when(col("DB.SN_1").eq(lit(db)))
        .then(col("ECN.SN_1"))
        .otherwise(lit(0))
        + when(col("DB.SN_2").eq(lit(db)))
            .then(col("ECN.SN_2"))
            .otherwise(lit(0))
        + when(col("DB.SN_3").eq(lit(db)))
            .then(col("ECN.SN_3"))
            .otherwise(lit(0))
}
