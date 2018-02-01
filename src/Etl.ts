import { Observable } from 'rxjs';

import { Extractor } from './interfaces/Extractor';
import { GeneralTransformer } from './interfaces/GeneralTransformer';
import { Loader } from './interfaces/Loader';
import { Transformer } from './interfaces/Transformer';
import { MapTransformer } from './transformers/MapTransformer';

export enum EtlState {
    Running,
    Stopped,
    Error,
}

/**
 * ETL Class. Instantiate one and add as many extractors, transformers and loaders as you want.
 * Then start the whole process with ".start()".
 *
 * This processor is modular, you can find other implemented loaders and extractors in the README
 */
export class Etl {
    private _extractors: Extractor[] = [];
    private _generalTransformers: GeneralTransformer[] = [];
    private _transformers: Transformer[] = [];
    private _loaders: Loader[] = [];
    private _state: EtlState = EtlState.Stopped;
    private _context: any = null;

    public constructor(context?: any) {
        this.setContext(context);
    }

    public get extractors(): Extractor[] {
        return this._extractors;
    }

    public get generalTransformers(): GeneralTransformer[] {
        return this._generalTransformers;
    }

    public get transformers(): Transformer[] {
        return this._transformers;
    }

    public get loaders(): Loader[] {
        return this._loaders;
    }

    public get state(): EtlState {
        return this._state;
    }

    public setContext(context: any): this {
        if (this._state !== EtlState.Stopped) {
            this._state = EtlState.Error;
            throw new Error('Tried to set context on invalid state.');
        }
        this._context = context;
        return this;
    }

    public addExtractor(extract: Extractor): Etl {
        this._extractors.push(extract);
        return this;
    }

    public addGeneralTransformer(transformer: GeneralTransformer): Etl {
        this._generalTransformers.push(transformer);
        return this;
    }

    public addTransformer(transformer: Transformer): Etl {
        this.addGeneralTransformer(new MapTransformer(transformer));
        this._transformers.push(transformer);
        return this;
    }

    public addLoader(loader: Loader): Etl {
        this._loaders.push(loader);
        return this;
    }

    /**
     * Starts the etl process. First, all extractors are run in parallel and deliver their results into an observable.
     * Once the buffer gets a result, it transfers all objects through the transformers (one by one).
     * After that, the transformed results are run through all loaders in parallel.
     *
     * @returns {Observable<any>} Observable that completes when the process is finished,
     *                            during the "next" process step you get update on how many are processed yet.
     *                            Throws when any step produces an error.
     */
    public start(observable: Observable<any> = Observable.empty()): Observable<any> {
        this._state = EtlState.Running;

        const o: Observable<any> = Observable
            .merge(observable, ...this._extractors.map(extractor => extractor.read(this._context)));

        return this._generalTransformers
            .reduce((observable, transformer) => transformer.process(observable, this._context), o)
            .flatMap(object => Observable.merge(...this._loaders.map(loader => loader.write(object, this._context))))
            .do(
            () => { },
            (err) => {
                this._state = EtlState.Error;
                return Observable.throw(err);
            },
            () => {
                this._state = EtlState.Stopped;
            },
        );
    }

    /**
     * Resets the whole Etl object. Deletes all modifiers and resets the state.
     */
    public reset(): void {
        this._extractors = [];
        this._transformers = [];
        this._loaders = [];
        this._state = EtlState.Stopped;
        this._context = null;
    }
}
