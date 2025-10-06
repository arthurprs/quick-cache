use memory_stats::memory_stats;
use plotters::prelude::*;
use quick_cache::sync::Cache;
use std::any::type_name_of_val;

fn main() {
    let cache_capacity = 2000 * 10000;
    let image_path = "cache_memory_used.png";

    type Key = [u8; 16];
    type Value = ();

    let cache: Cache<Key, Value> = Cache::new(cache_capacity);

    #[derive(Debug)]
    struct MemoryData {
        n: usize,
        process_physical_memory: usize,
        process_virtual_memory: usize,
        cache_used_memory: usize,
        cache_entries_memory: usize,
        cache_map_memory: usize,
    }

    let mut memory_datas = Vec::new();
    for n in 0..cache_capacity * 2 {
        if n % (10 * 10000) == 0 {
            let process_memory = memory_stats().unwrap();
            let cache_memory = cache.memory_used();
            let memory_data = MemoryData {
                n,
                process_physical_memory: process_memory.physical_mem / 1024 / 1024,
                process_virtual_memory: process_memory.virtual_mem / 1024 / 1024,
                cache_used_memory: cache_memory.total() / 1024 / 1024,
                cache_entries_memory: cache_memory.entries / 1024 / 1024,
                cache_map_memory: cache_memory.map / 1024 / 1024,
            };
            println!("{:?}", memory_data);
            memory_datas.push(memory_data);
        }
        let key: Key = (n as u128).to_le_bytes();
        cache.insert(key, ());
    }

    let root = BitMapBackend::new(&image_path, (2400, 1600)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let max_memory = memory_datas
        .iter()
        .map(|data| {
            data.process_physical_memory
                .max(data.process_virtual_memory)
                .max(data.cache_used_memory)
                .max(data.cache_entries_memory)
                .max(data.cache_map_memory)
        })
        .max()
        .unwrap_or(100);

    let y_max = (max_memory as f64 * 1.1) as usize;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!(
                "Memory Used ({}(cap={}))",
                type_name_of_val(&cache),
                cache_capacity
            ),
            ("sans-serif", 60),
        )
        .margin(40)
        .x_label_area_size(80)
        .y_label_area_size(120)
        .build_cartesian_2d::<_, _>(0..memory_datas.last().unwrap().n, 0..y_max)
        .unwrap();

    chart
        .configure_mesh()
        .x_desc("Insertion Steps")
        .y_desc("Memory Usage (MB)")
        .label_style(("sans-serif", 40))
        .axis_desc_style(("sans-serif", 40))
        .draw()
        .unwrap();

    macro_rules! draw_series {
        ($field:ident, $color:expr, $label:expr) => {
            chart
                .draw_series(LineSeries::new(
                    memory_datas.iter().map(|data| (data.n, data.$field)),
                    $color,
                ))
                .unwrap()
                .label($label)
                .legend(|(x, y)| {
                    PathElement::new(vec![(x, y), (x + 40, y)], $color.stroke_width(2))
                });
        };
    }
    draw_series!(process_physical_memory, RED, "Process Physical Memory");
    draw_series!(process_virtual_memory, BLUE, "Process Virtual Memory");
    draw_series!(cache_used_memory, GREEN, "Cache Used Memory");
    draw_series!(cache_entries_memory, BLACK, "Cache Entries Memory");
    draw_series!(cache_map_memory, MAGENTA, "Cache Map Memory");

    chart
        .configure_series_labels()
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .label_font(("sans-serif", 20))
        .legend_area_size(60)
        .draw()
        .unwrap();

    root.present().unwrap();

    println!("Plot has been saved to: {}", image_path);
}
