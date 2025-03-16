package com.ververica.flinktraining.exercises.datastream_java.state;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * Java-реализация упражнения "Stateful Enrichment" из тренинга по Flink.
 * Цель: обогатить данные о поездках такси (TaxiRides) информацией о тарифах (TaxiFares).
 * Параметры:
 * -rides путь-к-файлу-данных-поездок
 * -fares путь-к-файлу-данных-тарифов
 */
public class RidesAndFaresExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {
        // Парсинг параметров командной строки
        ParameterTool params = ParameterTool.fromArgs(args);
        final String ridesFile = params.get("rides", pathToRideData);
        final String faresFile = params.get("fares", pathToFareData);

        // Константы для управления задержкой и скоростью обработки
        final int delay = 60;               // Максимальная задержка событий до 60 секунд
        final int servingSpeedFactor = 1800; // События за 30 минут обрабатываются за 1 секунду

        // Настройка конфигурации для состояния и контрольных точек
        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");           // Используем файловую систему для хранения состояния
        conf.setString("state.savepoints.dir", "file:///tmp/savepoints"); // Путь для сохранения точек сохранения
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints"); // Путь для контрольных точек

        // Создание среды выполнения с веб-интерфейсом
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(ExerciseBase.parallelism); // Установка параллелизма

        // Включение контрольных точек каждые 10 секунд
        env.enableCheckpointing(10000L);
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // Сохранение контрольных точек при отмене

        // Создание потока данных поездок с фильтрацией начальных поездок
        DataStream<TaxiRide> rides = env
                .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
                .filter((TaxiRide ride) -> ride.isStart) // Оставляем только стартовые поездки
                .keyBy(ride -> ride.rideId);             // Группировка по rideId

        // Создание потока данных тарифов
        DataStream<TaxiFare> fares = env
                .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
                .keyBy(fare -> fare.rideId);             // Группировка по rideId

        // Соединение потоков и обогащение данных с использованием состояния
        DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
                .connect(fares)                          // Объединяем потоки поездок и тарифов
                .flatMap(new EnrichmentFunction())       // Применяем функцию обогащения
                .uid("enrichment");                     // Уникальный идентификатор для доступа к состоянию

        // Вывод результатов или тестирование
        printOrTest(enrichedRides);

        // Запуск программы
        env.execute("Join Rides with Fares (java RichCoFlatMap)");
    }

    /**
     * Функция обогащения для совместной обработки потоков поездок и тарифов.
     * Использует состояние для временного хранения данных до их сопоставления.
     */
    public static class EnrichmentFunction
            extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
        // Состояния для хранения поездок и тарифов
        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {
            // Инициализация состояний
            rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка элемента поездки
            TaxiFare fare = fareState.value();
            if (fare != null) {
                // Если тариф уже есть, соединяем и очищаем состояние
                fareState.clear();
                out.collect(new Tuple2<>(ride, fare));
            } else {
                // Если тарифа нет, сохраняем поездку в состояние
                rideState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
            // Обработка элемента тарифа
            TaxiRide ride = rideState.value();
            if (ride != null) {
                // Если поездка уже есть, соединяем и очищаем состояние
                rideState.clear();
                out.collect(new Tuple2<>(ride, fare));
            } else {
                // Если поездки нет, сохраняем тариф в состояние
                fareState.update(fare);
            }
        }
    }
}