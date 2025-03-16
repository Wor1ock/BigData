package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import com.ververica.flinktraining.solutions.datastream_java.windows.HourlyTipsSolution;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Упражнение "Hourly Tips" из тренинга по Flink.
 * Задача:
 * 1. Рассчитать сумму чаевых, собранных каждым водителем, по часам.
 * 2. Найти максимальную сумму чаевых за каждый час среди всех водителей.
 * Параметры:
 * -input путь-к-файлу-данных
 */
public class HourlyTipsExercise extends ExerciseBase {

    public static void main(String[] args) throws Exception {
        // Парсинг параметров командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToFareData);

        // Константы для управления задержкой и скоростью обработки
        final int maxEventDelay = 60;       // События могут быть неупорядочены до 60 секунд
        final int servingSpeedFactor = 600; // События за 10 минут обрабатываются за 1 секунду

        // Настройка среды выполнения потоковой обработки
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // Используем временные метки событий
        env.setParallelism(ExerciseBase.parallelism);                  // Устанавливаем параллелизм

        // Создание потока данных тарифов из источника
        DataStream<TaxiFare> fares = env.addSource(
                fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

        // Основная логика обработки:
        // 1. Группировка по водителям и часовым окнам
        // 2. Подсчет чаевых
        // 3. Поиск максимума по всем водителям в каждом часе
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
                .keyBy((TaxiFare fare) -> fare.driverId)           // Группировка по ID водителя
                .timeWindow(Time.hours(1))                         // Часовые окна
                .process(new AddTips())                            // Подсчет суммы чаевых для каждого водителя
                .timeWindowAll(Time.hours(1))                      // Объединяем все данные в часовое окно
                .maxBy(2);                                        // Находим максимум по сумме чаевых (индекс 2)

        // Вывод результатов или тестирование
        printOrTest(hourlyMax);

        // Запуск программы
        env.execute("Hourly Tips (java)");
    }

    /**
     * Функция обработки окна для подсчета суммы чаевых.
     * Выдает кортеж: (время окончания окна, ID водителя, сумма чаевых).
     */
    public static class AddTips extends ProcessWindowFunction<
            TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<TaxiFare> fares,
                            Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            Float sumOfTips = 0F; // Инициализация суммы чаевых
            // Подсчет общей суммы чаевых для всех записей в окне
            for (TaxiFare f : fares) {
                sumOfTips += f.tip;
            }
            // Вывод результата: время окончания окна, ID водителя, сумма чаевых
            out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
        }
    }
}