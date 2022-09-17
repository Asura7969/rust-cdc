




#[cfg(test)]
mod tests {

    #[test]
    fn open_table() {
        let table = deltalake::open_table("./tests/data/simple_table").await.unwrap();
        // println!("{}", table.get_files());
    }
}
