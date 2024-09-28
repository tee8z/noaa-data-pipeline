use std::collections::{BTreeMap, HashSet};

use itertools::Itertools;
use log::info;

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

fn generate_matrix(
    number_of_places_win: usize,
    rankings: Vec<Vec<usize>>,
    total_allowed_entries: usize,
) -> Vec<BTreeMap<usize, Vec<usize>>> {
    let mut entry_indices: Vec<usize> = (0..total_allowed_entries).collect();
    entry_indices.reverse();
    let possible_indices: Vec<Vec<usize>> = generate_all_combinations(entry_indices.clone());
    println!("index_list {:?}", possible_indices);
    let mut possible_outcomes: Vec<BTreeMap<usize, Vec<usize>>> = Vec::new();
    for (ranking, indices) in rankings.iter().zip(possible_indices.iter()) {
        let mut current_map = BTreeMap::new();
        let mut used_indices = HashSet::new();

        // Process each rank and corresponding indices
        for rank in ranking {
            let mut valid_indices = Vec::new();

            // Add valid indices that are not yet used
            for index in indices.clone() {
                if !used_indices.contains(&index) {
                    valid_indices.push(index);
                    used_indices.insert(index);
                }
            }

            // Insert into the BTreeMap if valid indices exist
            if !valid_indices.is_empty() {
                current_map.insert(rank.clone(), valid_indices.clone());
            }
        }

        possible_outcomes.push(current_map);
    }

    possible_outcomes
    /*
    let number_of_indice_permuations = possible_indices.len();

    for (item_index, entry_index_list) in possible_indices.iter().enumerate() {
        if entry_index_list.is_empty() {
            continue;
        }

        // Pair each ranking with the current index list
        cur_index_options = Some(entry_index_list.clone());

        for ranking in &rankings {
            let mut map = BTreeMap::new();
            for rank in ranking {

                //spread out index
            }
            // Skip 0 score until the last permuation of indicies
            if ranking.len() == 1
                && ranking[0] == 0
                && item_index != (number_of_indice_permuations - 1)
            {
                continue;
            }
            // Use the current ranking and index list
            map.insert(ranking[0], entry_index_list.clone());
            possible_outcomes.push(map);
        }
        prev_index_options = cur_index_options;
    }

    possible_outcomes
    */
}

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

    use super::{generate_matrix, generate_possible_outcome_rankings};

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
        let rankings = generate_possible_outcome_rankings(number_of_places_win, vec![0, 1, 2, 3]);
        println!("rankings {:?}", rankings);
        let matrix = generate_matrix(number_of_places_win, rankings, total_allowed_entries);
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
    fn can_generate_possible_outcome_rankings_three_winners() {
        let expect_possible_outcome_rankings = [
            vec![0],
            vec![1],
            vec![2],
            vec![3],
            vec![0, 1],
            vec![0, 2],
            vec![0, 3],
            vec![1, 2],
            vec![1, 3],
            vec![2, 3],
            vec![0, 1, 2],
            vec![0, 1, 3],
            vec![0, 2, 3],
            vec![1, 2, 3],
        ];
        let number_of_places_win = 3;
        let possible_score = vec![0, 1, 2, 3];
        let rankings = generate_possible_outcome_rankings(number_of_places_win, possible_score);
        println!("expected_rankings {:?}", expect_possible_outcome_rankings);
        println!("rankings {:?}", rankings);
        assert_eq!(
            to_sorted_set(rankings),
            to_sorted_set(expect_possible_outcome_rankings.to_vec())
        );
    }

    fn to_sorted_set(vec: Vec<Vec<usize>>) -> HashSet<Vec<usize>> {
        vec.into_iter()
            .map(|mut v| {
                v.sort();
                v
            })
            .collect()
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
        let possible_score = vec![0, 1, 2, 3];
        let rankings = generate_possible_outcome_rankings(number_of_places_win, possible_score);
        println!("rankings {:?}", rankings);
        let matrix = generate_matrix(number_of_places_win, rankings, total_allowed_entries);
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
