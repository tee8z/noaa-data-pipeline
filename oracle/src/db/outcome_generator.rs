use std::collections::{BTreeMap, HashMap, HashSet};

use itertools::Itertools;
use log::info;

pub fn generate_ranked_players(
    number_of_places_win: usize,
    total_allowed_entries: usize,
) -> Vec<BTreeMap<usize, Vec<usize>>> {
    // add another place to generate the "unranked" values
    let matrix = generate_partitions(total_allowed_entries, number_of_places_win + 1);
    println!("matrix: {:?}", matrix);
    let mut permutations = vec![];

    for permutation in matrix {
        let mut game_results = BTreeMap::new();

        //can never have first place be empty with later ranks having a value
        if permutation[0].len() == 0 {
            continue;
        }
        for (rank, player_indexies) in permutation.iter().enumerate() {
            game_results.insert(rank + 1, player_indexies.clone());
        }
        permutations.push(game_results);
    }
    permutations
}

fn generate_partitions(n: usize, k: usize) -> Vec<Vec<Vec<usize>>> {
    let mut partitions = Vec::new();

    // Generate all partitions of `n` players into up to `k` groups
    for num_groups in 1..=k {
        // Generate all ways to partition the players into exactly `num_groups` groups
        let partition_iter = (0..n).combinations(num_groups - 1);
        for breaks in partition_iter {
            let mut partition = vec![Vec::new(); num_groups];
            let mut current_group = 0;
            for player in 0..n {
                if breaks.contains(&player) {
                    current_group += 1;
                }
                partition[current_group].push(player);
            }
            partitions.push(partition);
        }
    }

    partitions
}

pub fn generate_outcome_messages(
    possible_user_outcomes: Vec<BTreeMap<usize, Vec<usize>>>,
) -> Vec<Vec<u8>> {
    possible_user_outcomes
        .into_iter()
        .map(|inner_vec| {
            inner_vec
                .into_iter()
                .flat_map(|(_, values)| {
                    values
                        .iter()
                        .flat_map(|val| val.to_be_bytes())
                        .collect::<Vec<_>>()
                })
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashSet};

    use maplit::btreemap;

    use crate::generate_ranked_players;

    use super::generate_partitions;

    #[test]
    fn can_generate_matrix_permutations() {
        let total_allowed_entries = 3; // Number of players
        let number_of_places_win = 2; // We are only capturing the top 3 rankings
        let matrix = generate_partitions(total_allowed_entries, number_of_places_win);
        println!("matrix: {:?}", matrix);
        let ranked = generate_ranked_players(number_of_places_win, total_allowed_entries);
        println!("ranked: {:?}", ranked);
        assert_eq!(matrix.is_empty(), false);
    }

    #[test]
    fn can_generate_matrix_one_winning_rank() {
        let expected: Vec<BTreeMap<usize, Vec<usize>>> = vec![
            btreemap! {
                3=>vec![2],
            },
            btreemap! {
                2=>vec![2],
            },
            btreemap! {
                1=>vec![2],
            },
            btreemap! {
                3=>vec![1],
            },
            btreemap! {
                2=>vec![1],
            },
            btreemap! {
                1=>vec![1],
            },
            btreemap! {
                3=>vec![0],
            },
            btreemap! {
                2=>vec![0],
            },
            btreemap! {
                1=>vec![0],
            },
            btreemap! {
                3=>vec![1,2],
            },
            btreemap! {
                2=>vec![1,2],
            },
            btreemap! {
                1=>vec![1,2],
            },
            btreemap! {
                3=>vec![0,2],
            },
            btreemap! {
                2=>vec![0,2],
            },
            btreemap! {
                1=>vec![0,2],
            },
            btreemap! {
                3=>vec![0,1],
            },
            btreemap! {
                2=>vec![0,1],
            },
            btreemap! {
                1=>vec![0,1],
            },
            btreemap! {
                3=>vec![0,1,2],
            },
            btreemap! {
                2=>vec![0,1,2],
            },
            btreemap! {
                1=>vec![0,1,2],
            },
            btreemap! {
                0=>vec![0,1,2]
            },
        ];

        let total_allowed_entries = 3;
        let number_of_places_win = 1;

        let matrix = generate_ranked_players(number_of_places_win, total_allowed_entries);
        let mut matrix_iter = matrix.iter();
        println!("matrix {:?}", matrix);
        for outcome in expected {
            println!("expected_outcome {:?}", outcome);
            let result = matrix_iter.find(|possible_outcome| **possible_outcome == outcome);
            println!("result {:?}", result);
            assert_ne!(result, None);
            assert_eq!(*(result.unwrap()), outcome);
        }
    }

    #[test]
    fn can_generate_matrix_three_winning_ranks() {
        let expected_matrix: Vec<BTreeMap<usize, Vec<usize>>> = vec![
            btreemap! {
                3=>vec![0,1,2],
            },
            btreemap! {
                3=>vec![1,2,3],
            },
            btreemap! {
                3=>vec![0,2,3],
            },
            btreemap! {
                3=>vec![0,1,2,3],
            },
            //////////////
            btreemap! {
                2=>vec![0,1,2],
            },
            btreemap! {
                2=>vec![1,2,3],
            },
            btreemap! {
                2=>vec![0,2,3],
            },
            btreemap! {
                2=>vec![0,1,2,3],
            },
            //////////////
            btreemap! {
                1=>vec![0,1,2],
            },
            btreemap! {
                1=>vec![1,2,3],
            },
            btreemap! {
                1=>vec![0,2,3],
            },
            btreemap! {
                1=>vec![0,1,2,3],
            },
            //////////////
            btreemap! {
                0=>vec![0,1,2,3],
            },
            //////////////
            btreemap! {
                3=>vec![3],
                2=>vec![0,1],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![0,2],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![1,2],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![0,1,2],
            },
            ////------
            btreemap! {
                3=>vec![2],
                2=>vec![0,3],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![0,1],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![1,3],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![0,1,3],
            },
            ////------
            btreemap! {
                3=>vec![1],
                2=>vec![0,3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0,2],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![2,3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0,2,3],
            },
            ////------
            btreemap! {
                3=>vec![0],
                2=>vec![1,3],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![1,2],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![2,3],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![1,2,3],
            },
            ////------
            btreemap! {
                3=>vec![0,2,3],
                2=>vec![1],
            },
            btreemap! {
                3=>vec![0,1,3],
                2=>vec![2],
            },
            btreemap! {
                3=>vec![0,1,2],
                2=>vec![3],
            },
            //////////////
            btreemap! {
                3=>vec![3],
                1=>vec![1,2],
            },
            btreemap! {
                3=>vec![3],
                1=>vec![0,2],
            },
            btreemap! {
                3=>vec![3,1],
                1=>vec![0,2],
            },
            //////////////
            btreemap! {
                3=>vec![3],
                0=>vec![1,2]
            },
            btreemap! {
                3=>vec![3],
                0=>vec![0,2]
            },
            //////////////
            btreemap! {
                2=>vec![3],
                1=>vec![0,1,2],
            },
            btreemap! {
                2=>vec![3],
                1=>vec![1,2],
            },
            btreemap! {
                2=>vec![3],
                1=>vec![0,2],
            },
            //////////////
            btreemap! {
                2=>vec![3],
                0=>vec![1,2]
            },
            btreemap! {
                2=>vec![3],
                0=>vec![0,2]
            },
            //////////////
            btreemap! {
                1=>vec![3],
                0=>vec![1,2]
            },
            btreemap! {
                1=>vec![3],
                0=>vec![0,2]
            },
            btreemap! {
                1=>vec![3],
                0=>vec![0,1,2]
            },
            btreemap! {
                1=>vec![2],
                0=>vec![1,3]
            },
            btreemap! {
                1=>vec![2],
                0=>vec![0,3]
            },
            btreemap! {
                1=>vec![2],
                0=>vec![0,1,3]
            },
            btreemap! {
                1=>vec![1],
                0=>vec![2,3]
            },
            btreemap! {
                1=>vec![1],
                0=>vec![0,2]
            },
            btreemap! {
                1=>vec![1],
                0=>vec![0,2,3]
            },
            btreemap! {
                1=>vec![0],
                0=>vec![2,3]
            },
            btreemap! {
                1=>vec![0],
                0=>vec![1,3]
            },
            btreemap! {
                1=>vec![0],
                0=>vec![1,2,3]
            },
            btreemap! {
                1=>vec![0,1],
                0=>vec![2,3]
            },
            btreemap! {
                1=>vec![1,2],
                0=>vec![0,3]
            },
            btreemap! {
                1=>vec![0,2,3],
                0=>vec![1]
            },
            btreemap! {
                1=>vec![1,2,3],
                0=>vec![0]
            },
            btreemap! {
                1=>vec![1,0,3],
                0=>vec![2]
            },
            btreemap! {
                1=>vec![1,2,0],
                0=>vec![3]
            },
            //////////////
            btreemap! {
                3=>vec![3],
                2=>vec![2],
                1=>vec![1],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![1],
                1=>vec![2],
            },
            btreemap! {
                3=>vec![3,0],
                2=>vec![2],
                1=>vec![1],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![2,0],
                1=>vec![1],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![2],
                1=>vec![1,0],
            },
            ////------
            btreemap! {
                3=>vec![2],
                2=>vec![1],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![3],
                1=>vec![1],
            },
            btreemap! {
                3=>vec![2,0],
                2=>vec![3],
                1=>vec![1],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![3,0],
                1=>vec![1],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![3],
                1=>vec![1,0],
            },
            ////------++
            btreemap! {
                3=>vec![2],
                2=>vec![3],
                1=>vec![0],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![0],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![2,1],
                2=>vec![0],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![0,1],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![0],
                1=>vec![3,1],
            },
            ////------
            btreemap! {
                3=>vec![1],
                2=>vec![2],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![3],
                1=>vec![2],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0],
                1=>vec![2],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![2],
                1=>vec![0],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![3],
                1=>vec![0],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0,2],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0],
                1=>vec![3,2],
            },
            ////------
            btreemap! {
                3=>vec![0],
                2=>vec![3],
                1=>vec![2],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![2],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![3],
                1=>vec![1],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![1],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![1,2],
                1=>vec![3],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![1],
                1=>vec![3,2],
            },
            //////////////
            btreemap! {
                3=>vec![3],
                2=>vec![0],
                0=>vec![2],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![2],
                0=>vec![1],
            },
            btreemap! {
                3=>vec![3],
                2=>vec![1],
                0=>vec![2]
            },
            btreemap! {
                3=>vec![2],
                2=>vec![1],
                0=>vec![3]
            },
            btreemap! {
                3=>vec![2],
                2=>vec![3],
                0=>vec![1]
            },
            btreemap! {
                3=>vec![2],
                2=>vec![3],
                0=>vec![0],
            },
            btreemap! {
                3=>vec![2],
                2=>vec![0],
                0=>vec![3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![2],
                0=>vec![3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![3],
                0=>vec![2],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0],
                0=>vec![2],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![2],
                0=>vec![0],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![0],
                0=>vec![3],
            },
            btreemap! {
                3=>vec![1],
                2=>vec![3],
                0=>vec![0],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![3],
                0=>vec![2],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![2],
                0=>vec![3],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![3],
                0=>vec![1],
            },
            btreemap! {
                3=>vec![0],
                2=>vec![1],
                0=>vec![3],
            },
            //////////////
            btreemap! {
                2=>vec![3],
                1=>vec![2],
                0=>vec![1],
            },
            btreemap! {
                2=>vec![3],
                1=>vec![1],
                0=>vec![2]
            },
            btreemap! {
                2=>vec![2],
                1=>vec![1],
                0=>vec![3]
            },
            btreemap! {
                2=>vec![2],
                1=>vec![3],
                0=>vec![1]
            },
            btreemap! {
                2=>vec![2],
                1=>vec![3],
                0=>vec![0],
            },
            btreemap! {
                2=>vec![2],
                1=>vec![0],
                0=>vec![3],
            },
            btreemap! {
                2=>vec![1],
                1=>vec![2],
                0=>vec![3],
            },
            btreemap! {
                2=>vec![1],
                1=>vec![3],
                0=>vec![2],
            },
            btreemap! {
                2=>vec![1],
                1=>vec![0],
                0=>vec![2],
            },
            btreemap! {
                2=>vec![1],
                1=>vec![2],
                0=>vec![0],
            },
            btreemap! {
                2=>vec![1],
                1=>vec![0],
                0=>vec![3],
            },
            btreemap! {
                2=>vec![1],
                1=>vec![3],
                0=>vec![0],
            },
            btreemap! {
                2=>vec![0],
                1=>vec![3],
                0=>vec![2],
            },
            btreemap! {
                2=>vec![0],
                1=>vec![2],
                0=>vec![3],
            },
            btreemap! {
                2=>vec![0],
                1=>vec![3],
                0=>vec![1],
            },
            btreemap! {
                2=>vec![0],
                1=>vec![1],
                0=>vec![3],
            },
            //////////////
        ];
        let total_allowed_entries = 4;
        let number_of_places_win = 3;
        let matrix = generate_ranked_players(number_of_places_win, total_allowed_entries);
        let mut matrix_iter = matrix.iter();
        println!("matrix {:?}", matrix);
        for outcome in expected_matrix {
            println!("expected_outcome {:?}", outcome);
            let result = matrix_iter.find(|possible_outcome| **possible_outcome == outcome);
            println!("result {:?}", result);
            assert_ne!(result, None);
            assert_eq!(*(result.unwrap()), outcome);
        }
    }
}
