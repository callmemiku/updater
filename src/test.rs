macro_rules! either {
    ($test:expr => $true_expr:expr; $false_expr:expr) => {
        if $test {
            $true_expr
        }
        else {
            $false_expr
        }
    }
}

pub fn main() {
    let aboba = "aboba";
    let a = either!(aboba.is_empty() => "".to_owned(); "AND ".to_owned() + aboba);
    println!("{}", a);
}