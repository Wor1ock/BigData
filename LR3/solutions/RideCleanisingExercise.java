package com.ververica.flinktraining.exercises.datastream_java.basics;

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Упражнение "Ride Cleansing" из тренинга по Flink.
 * Задача: отфильтровать поток данных о поездках такси, оставив только те,
 * которые начинаются и заканчиваются в пределах Нью-Йорка (NYC).
 * Результат должен быть выведен на печать.
 * Параметры:
 * -input путь-к-файлу-данных
 */
public class RideCleansingExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {
        // Парсинг параметров командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        System.out.println("point 0"); // Точка отладки: параметры прочитаны

        // Константы для управления задержкой и скоростью обработки
        final int maxEventDelay = 60;       // События могут быть неупорядочены до 60 секунд
        final int servingSpeedFactor = 600; // События за 10 минут обрабатываются за 1 секунду

        System.out.println("point 1"); // Точка отладки: константы установлены

        // Настройка среды выполнения потоковой обработки
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT); // Установка параллелизма по умолчанию

        System.out.println("point 2"); // Точка отладки: среда настроена

        // Создание потока данных поездок из источника
        DataStream<TaxiRide> rides = env.addSource(
                rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

        System.out.println("point 3"); // Точка отладки: поток данных создан

        // Фильтрация поездок, начинающихся и заканчивающихся в NYC
        DataStream<TaxiRide> filteredRides = rides
                .filter(new NYCFilter()); // Применение фильтра

        System.out.println("point 4"); // Точка отладки: фильтрация завершена

        // Вывод отфильтрованного потока или тестирование
        printOrTest(filteredRides);

        // Запуск программы
        env.execute("Taxi Ride Cleansing");
    }

    /**
     * Фильтр для проверки, находятся ли начальная и конечная точки поездки в NYC.
     * Использует утилиту GeoUtils для географической проверки.
     */
    public static class NYCFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            // Проверка: начальная и конечная точки поездки должны быть в NYC
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}