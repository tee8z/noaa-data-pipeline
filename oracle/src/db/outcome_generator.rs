use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;
use log::info;

/*
pub fn generate_outcome_matrix(
    number_of_values_per_entry: usize,
    number_of_places_win: usize,
    total_allowed_entries: usize,
) -> Vec<BTreeMap<usize, Vec<usize>>> {
    // number_of_values_per_entry * 2 == max value
    let max_number_of_points_per_value_in_entry = 2;

    let possible_scores: Vec<usize> =
        (0..=(number_of_values_per_entry * max_number_of_points_per_value_in_entry)).collect();

    // allows us to have comps where say the top 3 scores split the pot
    let possible_outcome_rankings: Vec<Vec<usize>> =
        generate_possible_outcome_rankings(number_of_places_win, possible_scores);
    info!("possible ranking outcomes: {:?}", possible_outcome_rankings);

    generate_matrix(
        number_of_places_win,
        possible_outcome_rankings,
        total_allowed_entries,
    )
}

fn generate_possible_outcome_rankings(
    number_of_places_win: usize,
    mut possible_scores: Vec<usize>,
) -> Vec<Vec<usize>> {
    possible_scores.sort();
    possible_scores.reverse();
    let mut outcome_rankings = vec![];
    for length in 1..=possible_scores.len() {
        if length > number_of_places_win {
            break;
        }

        // For each possible length of subsets, iterate through combinations
        for subset in possible_scores.iter().copied().combinations(length) {
            outcome_rankings.push(subset);
        }
    }
    outcome_rankings
}
*/

// Function to generate all partitions (rankings) for k players, considering ties
fn generate_rankings_with_ties(num_ranks: usize, max_rank: usize) -> Vec<Vec<usize>> {
    fn backtrack(
        start: usize,
        remaining: usize,
        current: &mut Vec<usize>,
        result: &mut Vec<Vec<usize>>,
        max_rank: usize,
    ) {
        if remaining == 0 {
            result.push(current.clone());
            return;
        }
        for i in start..=max_rank {
            current.push(i);
            backtrack(i, remaining - 1, current, result, max_rank);
            current.pop();
        }
    }

    let mut result = Vec::new();
    let mut current = Vec::new();
    backtrack(1, num_ranks, &mut current, &mut result, max_rank);
    result
}

// Function to generate all combinations of k players from n players
fn combinations(n: usize, k: usize) -> Vec<Vec<usize>> {
    fn combine_helper(
        n: usize,
        k: usize,
        start: usize,
        current: &mut Vec<usize>,
        result: &mut Vec<Vec<usize>>,
    ) {
        if current.len() == k {
            result.push(current.clone());
            return;
        }
        for i in start..n {
            current.push(i);
            combine_helper(n, k, i + 1, current, result);
            current.pop();
        }
    }

    let mut result = Vec::new();
    let mut current = Vec::new();
    combine_helper(n, k, 0, &mut current, &mut result);
    result
}

// Main function to generate the BTreeMap<Vec<usize>> for ranked players
pub fn generate_ranked_players(
    total_allowed_entries: usize,
    number_of_places_win: usize,
    max_rank: usize,
) -> Vec<BTreeMap<usize, Vec<usize>>> {
    let mut results = Vec::new();

    // Step 1: Generate all combinations of number_of_places_win from total_allowed_entries
    let player_combinations = combinations(total_allowed_entries, number_of_places_win);

    // Step 2: Generate all possible rankings with ties fo number_of_places_win
    let ranking_combinations = generate_rankings_with_ties(number_of_places_win, max_rank);

    // Step 3: For each combination of players and each ranking, assign players to ranks
    for players in player_combinations {
        for ranking in &ranking_combinations {
            let mut rank_map: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
            for (i, &rank) in ranking.iter().enumerate() {
                rank_map.entry(rank).or_default().push(players[i]);
            }
            results.push(rank_map);
        }
    }

    results
}

/*
fn generate_all_combinations(elements: Vec<usize>) -> Vec<Vec<usize>> {
    let mut all_combinations = Vec::new();
    let n = elements.len();

    for size in 0..=n {
        let permutations = combinations(&elements, size);
        if permutations.len() == 0 {
            continue;
        }
        if permutations.len() == 1 {
            if permutations[0].len() == 0 {
                continue;
            }
        }
        all_combinations.extend(permutations);
    }

    all_combinations
}
*/
/*
fn combinations<T: Clone>(elements: &[T], n: usize) -> Vec<Vec<T>> {
    if n == 0 {
        return vec![vec![]]; // Base case: only the empty combination
    }

    if elements.is_empty() {
        return vec![]; // No combinations can be formed
    }

    let head = &elements[0];
    let tail = &elements[1..];

    // Combine head with combinations from the tail
    let mut with_head = combinations(tail, n - 1);
    for combo in &mut with_head {
        combo.push(head.clone());
    }

    // Combine without head
    let without_head = combinations(tail, n);

    // Combine results
    let mut result = with_head;
    result.extend(without_head);

    result
}*/

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

    use super::generate_ranked_players;

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
        let max_ranking = 3;

        let matrix =
            generate_ranked_players(number_of_places_win, total_allowed_entries, max_ranking);
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
        let max_ranking = 3;
        let matrix =
            generate_ranked_players(number_of_places_win, total_allowed_entries, max_ranking);
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
